package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func Consumer() {
	// Kafka broker address
	broker := "localhost:9093" // Change to your Kafka broker address
	topic := "test_topic"      // Topic to consume messages from

	// Set up the Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a new consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{broker}, "consumer-group1", config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Create a new consumer handler
	handler := &ConsumerGroupHandler{}

	// Start consuming messages
	for {
		if err := consumerGroup.Consume(context.Background(), []string{topic}, handler); err != nil {
			log.Fatalf("Error in consumer group: %v", err)
		}
	}
}

// ConsumerGroupHandler implements the sarama.ConsumerGroupHandler interface
type ConsumerGroupHandler struct{}

// Setup is called before consuming messages (can be used for setup tasks)
func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group session setup.")
	return nil
}

// Cleanup is called after consuming messages (can be used for cleanup tasks)
func (ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group session cleanup.")
	return nil
}

// ConsumeClaim is called when a new message is consumed
func (ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Iterate over messages in the claim
	for message := range claim.Messages() {
		// Print the received message
		fmt.Printf("Consumed message: %s\n, Partition: %d, Offset: %d", string(message.Value), message.Partition, message.Offset)
		// Mark the message as processed
		session.MarkMessage(message, "")
	}

	return nil
}
