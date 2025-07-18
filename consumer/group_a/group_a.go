package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// ConsumerGroupA represents a Sarama consumer group consumer for Group A
type ConsumerGroupA struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *ConsumerGroupA) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *ConsumerGroupA) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (c *ConsumerGroupA) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE: Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Println("[GROUP A] Message channel closed")
				return nil
			}

			// Process the message
			log.Printf("[GROUP A] Message received: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s",
				message.Topic, message.Partition, message.Offset,
				string(message.Key), string(message.Value))

			// Mark message as processed
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func main() {
	// Kafka configuration
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Consumer group configuration
	brokers := []string{"localhost:9092"}
	topics := []string{"test-topic"}
	consumerGroup := "consumer-group-A" // Different consumer group

	// Create consumer group
	consumer := &ConsumerGroupA{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, consumerGroup, config)
	if err != nil {
		log.Fatalf("Error creating consumer group A client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop
			if err := client.Consume(ctx, topics, consumer); err != nil {
				log.Printf("[GROUP A] Error from consumer: %v", err)
			}
			// Check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("[GROUP A] Kafka consumer started - will consume from partitions 0 and 1")

	// Handle graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Keep consuming until interrupted
	select {
	case <-ctx.Done():
		log.Println("[GROUP A] Terminating: context cancelled")
	case <-sigterm:
		log.Println("[GROUP A] Terminating: via signal")
	}

	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		log.Printf("[GROUP A] Error closing client: %v", err)
	}
}
