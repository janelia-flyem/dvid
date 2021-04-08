package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// KafkaTopicPrefix is the kafka topic prefix for mutation logging
	KafkaTopicPrefix string
)

var (
	// global producer
	kafkaProducer *kafka.Producer

	// the kafka topic for activity logging
	kafkaActivityTopic string

	// topic suffixes per data UUID for mutation logging
	kafkaTopicSuffixes map[dvid.UUID]string

	// have topics been created and verified
	kafkaTopicExists   map[string]bool
	kafkaTopicExistsMu sync.RWMutex
)

// assume very low throughput needed and therefore always one partition
const partitionID = 0

// KafkaMaxMessageSize is the max message size in bytes for a Kafka message.
const KafkaMaxMessageSize = 980 * dvid.Kilo

// KafkaConfig describes kafka servers and an optional local file directory into which
// failed messages will be stored.
type KafkaConfig struct {
	TopicActivity  string   // if supplied, will be override topic for activity log
	TopicPrefix    string   // if supplied, will be prefixed to any mutation logging
	TopicSuffixes  []string // optional topic suffixes per data UUID
	Servers        []string
	SecProtocol    string
	SASLMechanisms string
	SASLUsername   string
	SASLPassword   string
	BufferSize     int // queue.buffering.max.messages
}

// KafkaTopicSuffix returns any configured suffix for the given data UUID or the empty string.
func KafkaTopicSuffix(dataUUID dvid.UUID) string {
	if len(kafkaTopicSuffixes) == 0 {
		return ""
	}
	suffix, found := kafkaTopicSuffixes[dataUUID]
	if !found {
		return ""
	}
	return suffix
}

// Initialize sets up default activity topic and launches goroutine for handling async kafka messages.
func (kc KafkaConfig) Initialize(hostID string) error {
	if len(kc.Servers) == 0 {
		return nil
	}
	kafkaTopicExists = make(map[string]bool)
	kafkaTopicSuffixes = make(map[dvid.UUID]string)
	for _, spec := range kc.TopicSuffixes {
		parts := strings.Split(spec, ":")
		if len(parts) != 2 {
			dvid.Infof("Ignored bad kafka topic suffix specification (expected uuid:suffix): %s\n", spec)
		} else {
			kafkaTopicSuffixes[dvid.UUID(parts[0])] = parts[1]
		}
	}

	if kc.TopicPrefix != "" {
		KafkaTopicPrefix = kc.TopicPrefix
	}

	if kc.TopicActivity != "" {
		kafkaActivityTopic = kc.TopicActivity
	} else {
		kafkaActivityTopic = "dvidactivity-" + hostID
	}
	reg, err := regexp.Compile("[^a-zA-Z0-9\\._\\-]+")
	if err != nil {
		return err
	}
	kafkaActivityTopic = reg.ReplaceAllString(kafkaActivityTopic, "-")

	configMap := kafka.ConfigMap{
		"client.id":         "dvid-kafkaclient",
		"bootstrap.servers": strings.Join(kc.Servers, ","),
	}
	if kc.SecProtocol != "" {
		configMap["security.protocol"] = kc.SecProtocol
	}
	if kc.SASLMechanisms != "" {
		configMap["sasl.mechanisms"] = kc.SASLMechanisms
	}
	if kc.SASLUsername != "" {
		configMap["sasl.username"] = kc.SASLUsername
	}
	if kc.SASLPassword != "" {
		configMap["sasl.password"] = kc.SASLPassword
	}
	if kc.BufferSize != 0 {
		configMap["queue.buffering.max.messages"] = kc.BufferSize
	}
	if kafkaProducer, err = kafka.NewProducer(&configMap); err != nil {
		return err
	}

	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					dvid.Errorf("Delivery failed to kafka (topic %s): %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Error)
				}
			}
		}
	}()
	return nil
}

// KafkaShutdown makes sure that the kafka queue is flushed before stopping.
func KafkaShutdown() {
	if kafkaProducer != nil {
		dvid.Infof("Shutting down kafka producer %q...\n", kafkaProducer.String())
		var numTries int
		for {
			queueRemain := kafkaProducer.Flush(15 * 1000) // wait for max 15 seconds
			dvid.Infof("Kafka queue remaining: %d\n", queueRemain)
			if queueRemain == 0 {
				break
			}
			numTries++
			if numTries == 4 {
				dvid.Criticalf("Flushed for 60 seconds but still have %d in kafka queue.  Aborting.\n", numTries)
				break
			}
		}
	}
}

// LogActivityToKafka publishes activity
func LogActivityToKafka(activity map[string]interface{}) {
	if kafkaActivityTopic != "" {
		go func() {
			jsonmsg, err := json.Marshal(activity)
			if err != nil {
				dvid.Errorf("unable to marshal activity for kafka logging: %v\n", err)
			}
			if err := KafkaProduceMsg(jsonmsg, kafkaActivityTopic); err != nil {
				dvid.Errorf("unable to publish activity to kafka activity topic: %v\n", err)
			}
		}()
	}
}

// KafkaProduceMsg sends a message to kafka
func KafkaProduceMsg(value []byte, topic string) error {
	if kafkaProducer != nil {
		if err := topicAvailable(topic); err != nil {
			return err
		}
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
			Timestamp:      time.Now(),
		}
		if err := kafkaProducer.Produce(kafkaMsg, nil); err != nil {
			dvid.Errorf("Error in sending message to kafka topic %q: %v\n", topic, err)

			// Store data in append-only log
			storeFailedMsg("failed-kafka-"+topic, value)

			// Notify via email at least once per 10 minutes
			notification := fmt.Sprintf("Error in kafka messaging to topic %q, partition id %d: %v\n", topic, partitionID, err)
			if err := dvid.SendEmail("Kafka Error", notification, nil, "kakfa"); err != nil {
				dvid.Errorf("couldn't send email about kafka error: %v\n", err)
			}

			return fmt.Errorf("cannot produce message to topic %q, partition %d: %s", topic, partitionID, err)
		}
	}
	return nil
}

func topicAvailable(topic string) error {
	if topic == "" {
		return fmt.Errorf("can't use empty topic name")
	}
	kafkaTopicExistsMu.RLock()
	val, found := kafkaTopicExists[topic]
	kafkaTopicExistsMu.RUnlock()
	if found {
		if val == false {
			return fmt.Errorf("unable to create topic %q [cached attempt]", topic)
		}
		return nil
	}
	kafkaTopicExistsMu.Lock()
	defer kafkaTopicExistsMu.Unlock()
	a, err := kafka.NewAdminClientFromProducer(kafkaProducer)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, err := time.ParseDuration("5s")
	if err != nil {
		return err
	}
	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 3,
			Config: map[string]string{
				"cleanup.policy": "compact",
				//"delete.retention.ms": "0",
				"max.message.bytes": "2097164",
			},
		}},
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		return err
	}
	kafkaTopicExists[topic] = true
	for _, result := range results {
		dvid.Infof("Create topic %q: %s\n", topic, result)
	}
	return nil
}

// if we have default log store, save the failed messages
func storeFailedMsg(topic string, msg []byte) {
	s, err := DefaultLogStore()
	if err != nil {
		dvid.Criticalf("unable to store failed kafka message to topic %q because no log store\n", topic)
		return
	}
	wl, ok := s.(WriteLog)
	if !ok {
		dvid.Criticalf("unable to store failed kafka message to topic %q because log store is not WriteLog\n", topic)
		return
	}
	if err := wl.TopicAppend(topic, LogMessage{Data: msg}); err != nil {
		dvid.Criticalf("unable to store failed kafka message to topic %q: %v\n", topic, err)
	}
}
