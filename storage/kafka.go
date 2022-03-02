package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/janelia-flyem/dvid/dvid"

	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/kafkapubsub"
)

var (
	// KafkaTopicPrefix is the kafka topic prefix for mutation logging
	KafkaTopicPrefix string
)

var (
	// kafkaServers
	kafkaServers []string

	// activity pubsub
	kafkaActivityTopic *pubsub.Topic

	// the kafka topic for activity logging
	kafkaActivityTopicName string

	// topic suffixes per data UUID for mutation logging
	kafkaTopicSuffixes map[dvid.UUID]string

	// topics per data UUID for mutation logging
	kafkaTopics   map[string]*pubsub.Topic
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
	kafkaTopics = make(map[string]*pubsub.Topic)
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

	config := kafkapubsub.MinimalConfig()
	if kafkaActivityTopic, err = kafkapubsub.OpenTopic(kc.Servers, config, kafkaActivityTopicName, nil); err != nil {
		return err
	}
	dvid.Infof("Finished with initial Kafka setup")
	return nil
}

// KafkaShutdown makes sure that the kafka queue is flushed before stopping.
func KafkaShutdown() {
	if kafkaServers != nil {
		if kafkaActivityTopic != nil {
			dvid.Infof("Shutting down kafka activity topic %q...\n", kafkaActivityTopicName)
			ctx := context.Background()
			kafkaActivityTopic.Shutdown(ctx)
		}
		for name, kafkaTopic := range kafkaTopics {
			dvid.Infof("Shutting down kafka mutation topic %q...\n", name)
			ctx := context.Background()
			kafkaTopic.Shutdown(ctx)
		}
	}
}

// LogActivityToKafka publishes activity
func LogActivityToKafka(activity map[string]interface{}) {
	if kafkaActivityTopic != nil {
		go func() {
			jsonmsg, err := json.Marshal(activity)
			if err != nil {
				dvid.Errorf("unable to marshal activity for kafka logging: %v\n", err)
			}
			ctx := context.Background()
			if err := kafkaActivityTopic.Send(ctx, &pubsub.Message{Body: jsonmsg}); err != nil {
				dvid.Errorf("unable to publish activity to kafka topic %q: %v\n", kafkaActivityTopicName, err)
			}
		}()
	}
}

// KafkaProduceMsg sends a message to kafka
func KafkaProduceMsg(value []byte, topicName string) (err error) {
	if kafkaTopics == nil {
		return nil
	}
	kafkaTopicsMu.RLock()
	topic, found := kafkaTopics[topicName]
	kafkaTopicsMu.RUnlock()
	if !found {
		kafkaTopicsMu.Lock()
		defer kafkaTopicsMu.Unlock()

		config := kafkapubsub.MinimalConfig()
		if topic, err = kafkapubsub.OpenTopic(kafkaServers, config, topicName, nil); err != nil {
			return err
		}
		kafkaTopics[topicName] = topic
	}
	ctx := context.Background()
	if err := topic.Send(ctx, &pubsub.Message{Body: value}); err != nil {
		dvid.Errorf("unable to publish data to kafka topic %q: %v\n", topicName, err)

		// Store data in append-only log
		storeFailedMsg("failed-kafka-"+topicName, value)

		// Notify via email at least once per 10 minutes
		notification := fmt.Sprintf("Error in kafka messaging to topic %q, partition id %d: %v\n", topicName, err)
		if err := dvid.SendEmail("Kafka Error", notification, nil, "kakfa"); err != nil {
			dvid.Errorf("couldn't send email about kafka error: %v\n", err)
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
