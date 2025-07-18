package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Kafka configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas to acknowledge
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Create producer
	brokers := []string{"localhost:9092"} // Kafka broker addresses
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()

	// Topic to produce messages to
	topic := "test-topic"

	// Handle graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Send messages
	messageCount := 0
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			messageCount++
			message := fmt.Sprintf("Message #%d - Hello from Go producer at %s",
				messageCount, time.Now().Format("2006-01-02 15:04:05"))

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", messageCount)),
				Value: sarama.StringEncoder(message),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("Failed to send message: %v", err)
			} else {
				log.Printf("Message sent successfully! Topic: %s, Partition: %d, Offset: %d",
					topic, partition, offset)
			}

		case <-signals:
			log.Println("Shutting down producer...")
			return
		}
	}
}
