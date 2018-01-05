// +build !clustered,!gcloud

package dvid

import (
	"fmt"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

// global broker
var broker *kafka.Broker

// assume very low throughput needed and therefore always one partition
const partitionID = 0

type KafkaConfig struct {
	Servers []string
}

// InitKafka intializes kafka connection.
func (c *KafkaConfig) Initialize() {
	if c == nil || len(c.Servers) == 0 {
		Infof("No Kafka server specified.\n")
		return
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
		Criticalf("cannot connect to kafka cluster: %s", err)
	}
}

// KafkaProduceMsg sends a message to kafka
func KafkaProduceMsg(msg []byte, topic string) error {
	if broker != nil {
		producer := broker.Producer(kafka.NewProducerConf())
		pmsg := &proto.Message{Value: msg}
		if _, err := producer.Produce(topic, partitionID, pmsg); err != nil {
			return fmt.Errorf("cannot produce message to %s:%d: %s", topic, partitionID, err)
		}
	}
	return nil
}
