package storage

import (
	"fmt"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

// global broker
var broker *kafka.Broker

// assume very low throughput needed and therefore always one partition
const partitionID = 0

// KafkaConfig describes kafka servers and an optional local file directory into which
// failed messages will be stored.
type KafkaConfig struct {
	Servers []string
}

// Initialize intializes kafka connection
func (c *KafkaConfig) Initialize() error {
	if c == nil || len(c.Servers) == 0 {
		dvid.TimeInfof("No Kafka server specified.\n")
		return nil
	}

	// create kafka connection
	conf := kafka.NewBrokerConf("dvid-kafkaclient")

	// note: assume server allows automatic topic creation
	// when sending a message to a non-existing topic
	conf.AllowTopicCreation = true

	// connect to kafka cluster
	var err error
	broker, err = kafka.Dial(c.Servers, conf)
	if err != nil {
		return fmt.Errorf("cannot connect to kafka cluster: %s", err)
	}
	return nil
}

// KafkaProduceMsg sends a message to kafka
func KafkaProduceMsg(msg []byte, topic string) error {
	if broker != nil {
		producer := broker.Producer(kafka.NewProducerConf())
		pmsg := &proto.Message{Value: msg}
		if _, err := producer.Produce(topic, partitionID, pmsg); err != nil {
			// Store data in append-only log
			storeFailedMsg("kafka-"+topic, msg)

			// Notify via email at least once per 10 minutes
			message := fmt.Sprintf("Error in kafka messaging to topic %q, partition id %q: %v\n", topic, partitionID, err)
			if err := dvid.SendEmail("Kafka Error", message, nil, "kakfa"); err != nil {
				dvid.Errorf("couldn't send email about kafka error: %v\n", err)
			}

			return fmt.Errorf("cannot produce message to %s:%d: %s", topic, partitionID, err)
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
