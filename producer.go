package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

func Producer() {
	// Set up the configuration for the Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // Enable success reporting
	config.Producer.Return.Errors = true    // Enable error reporting

	// Connect to the Kafka server (broker)
	broker := "localhost:9093" // Change this to your Kafka broker address
	producer, err := sarama.NewAsyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf("Error creating async producer: %v", err)
	}
	defer func() {
		// Close the producer on exit
		if err := producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}()

	// The topic to which we are sending the message
	topic := "test_topic"

	// Goroutine to handle success responses
	go func() {
		for msg := range producer.Successes() {
			fmt.Printf("Message sent successfully to partition %d with offset %d\n", msg.Partition, msg.Offset)
		}
	}()

	// Goroutine to handle errors
	go func() {
		for err := range producer.Errors() {
			fmt.Printf("Failed to send message: %v\n", err.Err)
		}
	}()

	// Send 10 messages to both partitions asynchronously
	for i := 0; i < 10; i++ {
		partition := int32(i % 2) // Alternate between partition 0 and 1
		message := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Value:     sarama.StringEncoder(fmt.Sprintf("Message %d to partition %d \n", i+1, partition)),
		}

		// Send the message
		producer.Input() <- message
	}

	// Wait for interrupt signal to exit gracefully
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting down producer...")
}
