package storage

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// global producer
	kafkaProducer *kafka.Producer

	// the kafka topic for activity logging
	kafkaActivityTopic string

	// the kafka topic prefix for mutation logging
	KafkaTopicPrefix string

	// topic suffixes per data UUID for mutation logging
	kafkaTopicSuffixes map[dvid.UUID]string
)

// assume very low throughput needed and therefore always one partition
const partitionID = 0

// KafkaConfig describes kafka servers and an optional local file directory into which
// failed messages will be stored.
type KafkaConfig struct {
	TopicActivity string   // if supplied, will be override topic for activity log
	TopicPrefix   string   // if supplied, will be prefixed to any mutation logging
	TopicSuffixes []string // optional topic suffixes per data UUID
	Servers       []string
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

	configMap := &kafka.ConfigMap{
		"client.id":         "dvid-kafkaclient",
		"bootstrap.servers": strings.Join(kc.Servers, ","),
	}
	if kafkaProducer, err = kafka.NewProducer(configMap); err != nil {
		return err
	}

	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					dvid.Errorf("Delivery failed to kafka servers: %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	return nil
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
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
			Timestamp:      time.Now(),
		}
		if err := kafkaProducer.Produce(kafkaMsg, nil); err != nil {
			// Store data in append-only log
			storeFailedMsg("kafka-"+topic, value)

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
