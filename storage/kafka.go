package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid/dvid"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// global producer
	kafkaProducer *kafka.Producer

	// description of host for kafka messaging
	kafkaTopicPrefix string
)

// assume very low throughput needed and therefore always one partition
const partitionID = 0

// KafkaConfig describes kafka servers and an optional local file directory into which
// failed messages will be stored.
type KafkaConfig struct {
	TopicPrefix string // if supplied, will be appended to topic
	Servers     []string
}

// Initialize intializes kafka connection
func (c *KafkaConfig) Initialize() (err error) {
	if c == nil || len(c.Servers) == 0 {
		dvid.TimeInfof("No Kafka server specified.\n")
		return nil
	}
	kafkaTopicPrefix = c.TopicPrefix

	configMap := &kafka.ConfigMap{
		"client.id":         "dvid-kafkaclient",
		"bootstrap.servers": strings.Join(c.Servers, ","),
	}
	if kafkaProducer, err = kafka.NewProducer(configMap); err != nil {
		return
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
	return
}

// KafkaProduceMsg sends a message to kafka
func KafkaProduceMsg(value []byte, topic string) error {
	if kafkaProducer != nil {
		if kafkaTopicPrefix != "" {
			topic = kafkaTopicPrefix + "-" + topic
		}
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
