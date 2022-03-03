package storage

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/Shopify/sarama"
)

var (
	// KafkaTopicPrefix is the kafka topic prefix for mutation logging
	KafkaTopicPrefix string
)

var (
	// kafkaServers
	kafkaServers []string

	// producer
	kafkaProducer sarama.SyncProducer

	// the kafka topic for activity logging
	kafkaActivityTopicName string

	// topic suffixes per data UUID for mutation logging
	kafkaTopicSuffixes map[dvid.UUID]string

	// topics per data UUID for mutation logging
	kafkaTopics   map[string]bool
	kafkaTopicsMu sync.RWMutex
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

// Initialize sets up default activity topic and support for on-the-fly mutation topics
func (kc KafkaConfig) Initialize(hostID string) error {
	if len(kc.Servers) == 0 {
		return nil
	}
	dvid.Infof("Trying to initialize kafka...")
	kafkaServers = kc.Servers
	kafkaTopics = make(map[string]bool)
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

	if kafkaProducer, err = sarama.NewSyncProducer(kc.Servers, nil); err != nil {
		return err
	}
	dvid.Infof("Finished with initial Kafka setup")
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
	if _, _, err = kafkaProducer.SendMessage(msg); err != nil {
		if topicName != kafkaActivityTopicName {
			storeFailedMsg(topicName, value)
		}
		return fmt.Errorf("error on sending to kafka topic %q: %v", topicName, err)
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
