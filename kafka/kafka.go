package kafka

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"sync"
)

// Topic names
//  EVENTS_SG_CID_#  # = company id. Note SG for SendGrid

var bootstrapServers = ""
var saslMechanisms = ""
var securityProtocol = ""
var saslUsername = ""
var saslPassword = ""
func Env() error {

	bootstrapServers = os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if "" == bootstrapServers {
		return fmt.Errorf("kafka.Env() error, missing environment variable KAFKA_BOOTSTRAP_SERVERS")
	}

	saslMechanisms = os.Getenv("KAFKA_SASL_MECHANISMS")  // PLAIN

	securityProtocol = os.Getenv("KAFKA_SECURITY_PROTOCOL")  // SASL_SSL

	saslUsername = os.Getenv("KAFKA_SASL_USERNAME")

	saslPassword = os.Getenv("KAFKA_SASL_PASSWORD")

	return nil
}

/*
 	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
			Partition: kafka.PartitionAny,
		},
		Value:          []byte(word),
	}, nil)

	if nil != err {
		fmt.Printf("Failed to send message: %v\n", err)
	}

  	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

 */
var kafkaMutex sync.Mutex
var kafkaProducers []*kafka.Producer
func GetKafkaProducer() (*kafka.Producer, error) {

	conf := kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	}

	if "" != saslMechanisms {
		conf.SetKey("sasl.mechanisms", saslMechanisms)
	}

	if "" != securityProtocol {
		conf.SetKey("security.protocol", securityProtocol)
	}

	if "" != saslUsername {
		conf.SetKey("sasl.username", saslUsername)
	}

	if "" != saslPassword {
		conf.SetKey("sasl.password", saslPassword)
	}

	p, err := kafka.NewProducer(&conf)  // 23MB

	if nil != err {
		return nil, fmt.Errorf("kafka.GetKafkaProducer() failed to instantiate kafka producer: %v", err)
	}

	kafkaMutex.Lock()
	defer kafkaMutex.Unlock()
	kafkaProducers = append(kafkaProducers, p)

	return p, nil
}

/*
    For consuming

	c.SubscribeTopics([]string{"TESTING"}, nil)

	var timeout = time.Second * 15
	for {
		msg, err := c.ReadMessage(timeout)
		timeout = time.Second * 3
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				break
			}

			fmt.Printf("Consumer error: %v %v (%v)\n", err, err.(kafka.Error).Code(), msg)
		}
	}
 */
var kafkaConsumers []*kafka.Consumer
func GetKafkaConsumer(groupId string) (*kafka.Consumer, error) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.GetKafkaConsumer() failed to instantiate kafka consumer: %v", err)
	}

	kafkaMutex.Lock()
	defer kafkaMutex.Unlock()
	kafkaConsumers = append(kafkaConsumers, c)

	return c, nil
}

func Cleanup() {

	if nil != kafkaProducers {
		for _, p := range kafkaProducers {
			p.Close()
		}

		kafkaProducers = nil
	}

	if nil != kafkaConsumers {
		for _, c := range kafkaConsumers {
			c.Close()
		}

		kafkaConsumers = nil
	}

}
