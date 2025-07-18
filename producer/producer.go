package main

import (
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
)

func main() {
	brokerList := []string{"localhost:9092"}
	topic := "my-topic"
	partition := int32(1)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Value:     sarama.StringEncoder("Hello, Kafka from partition 0!"),
	}

	partitionOut, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Fprintf(os.Stdout, "Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partitionOut, offset)
}
