package kafka

import (
	"fmt"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"github.com/stretchr/testify/assert"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"testing"
	"time"
)

func TestKafkaProduce2(t *testing.T) {
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

	err := Env()
	assert.NoError(t, err)
	fmt.Printf("mem before: %s\n", forwarderStats.GetMemUsageStr())
	_, err = GetKafkaProducer()
	assert.NoError(t, err)
	fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())

	defer Cleanup()
}

func TestKafkaProduce(t *testing.T) {
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

	err := Env()
	assert.NoError(t, err)
	p, err := GetKafkaProducer()
	assert.NoError(t, err)

	defer Cleanup()

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

	// Produce messages to topic (asynchronously)
	topic := "TESTING_123"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
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
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

}

func TestKafkaConsume(t *testing.T) {
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

	err := Env()
	assert.NoError(t, err)
	c, err := GetKafkaConsumer("myGroup")
	assert.NoError(t, err)

	defer Cleanup()

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
}