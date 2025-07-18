package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokerList := []string{"localhost:9092"}
	topic := "my-topic"
	partition := int32(0)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	master, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Failed to start Sarama consumer: %v", err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Printf("Failed to close consumer: %v", err)
		}
	}()

	partitionConsumer, err := master.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %v", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Printf("Failed to close partition consumer: %v", err)
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Consuming messages from partition 0...")
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Consumed message offset %d: %s\n", msg.Offset, string(msg.Value))
		case err := <-partitionConsumer.Errors():
			log.Printf("Error: %v", err)
		case <-sigchan:
			fmt.Println("Interrupt is detected. Exiting...")
			break ConsumerLoop
		}
	}
}
