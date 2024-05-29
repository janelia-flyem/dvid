package storage

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/Shopify/sarama"
)

var (
	// KafkaTopicPrefix is the kafka topic prefix for mutation logging
	KafkaTopicPrefix string
)

var (
	// producer
	kafkaProducer sarama.AsyncProducer

	// the kafka topic for activity logging
	kafkaActivityTopicName string

	// topic suffixes per data UUID for mutation logging
	kafkaTopicSuffixes map[dvid.UUID]string
)

// KafkaMaxMessageSize is the max message size in bytes for a Kafka message.
const KafkaMaxMessageSize = 980 * dvid.Kilo

// KafkaConfig describes kafka servers and an optional local file directory into which
// failed messages will be stored.
type KafkaConfig struct {
	TopicActivity string   // if supplied, will be override topic for activity log
	TopicPrefix   string   // if supplied, will be prefixed to any mutation logging
	TopicSuffixes []string // optional topic suffixes per data UUID
	Servers       []string
	BufferSize    int // queue.buffering.max.messages
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

// KafkaActivityTopic returns the topic name used for logging activity for this server.
func KafkaActivityTopic() string {
	return kafkaActivityTopicName
}

// Initialize sets up default activity topic and support for on-the-fly mutation topics
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
		kafkaActivityTopicName = kc.TopicActivity
	} else {
		kafkaActivityTopicName = "dvidactivity-" + hostID
	}
	reg, err := regexp.Compile(`[^a-zA-Z0-9\\._\\-]+`)
	if err != nil {
		return err
	}
	kafkaActivityTopicName = reg.ReplaceAllString(kafkaActivityTopicName, "-")

	config := sarama.NewConfig()
	config.Producer.MaxMessageBytes = KafkaMaxMessageSize
	if kafkaProducer, err = sarama.NewAsyncProducer(kc.Servers, config); err != nil {
		return err
	}

	go func() {
		for err := range kafkaProducer.Errors() {
			dvid.Errorf("error on kafka send: %v\n", err)
			if err.Msg.Topic != kafkaActivityTopicName {
				value, _ := err.Msg.Value.Encode()
				storeFailedMsg(err.Msg.Topic, value)
			}
		}
	}()
	dvid.Infof("Kafka topic for dvid activity: %s\n", kafkaActivityTopicName)
	dvid.Infof("Kafka topic prefix for mutations: %s\n", KafkaTopicPrefix)
	return nil
}

// KafkaShutdown makes sure that the kafka queue is flushed before stopping.
func KafkaShutdown() {
	if kafkaProducer != nil {
		if err := kafkaProducer.Close(); err != nil {
			dvid.Errorf("Kafka producer had error on close: %v\n", err)
		} else {
			dvid.Infof("Successfully shut down kafka producer.\n")
		}
		dvid.Infof("Kafka closed.")
	} else {
		dvid.Infof("Kafka producer was nil so unnecessary to close.")
	}
}

// LogActivityToKafka publishes activity
func LogActivityToKafka(activity map[string]interface{}) {
	if kafkaProducer != nil {
		go func() {
			jsonmsg, err := json.Marshal(activity)
			if err != nil {
				dvid.Errorf("unable to marshal activity for kafka logging: %v\n", err)
			}
			if err := KafkaProduceMsg(jsonmsg, kafkaActivityTopicName); err != nil {
				dvid.Errorf("unable to publish activity: %v\n", err)
			}
		}()
	}
}

// KafkaProduceMsg sends a message to kafka
func KafkaProduceMsg(value []byte, topicName string) (err error) {
	if kafkaProducer == nil {
		return nil
	}
	timeKey := sarama.StringEncoder(strconv.FormatInt(time.Now().UnixNano(), 10))
	msg := &sarama.ProducerMessage{Topic: topicName, Value: sarama.ByteEncoder(value), Key: timeKey}
	kafkaProducer.Input() <- msg
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
