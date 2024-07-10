package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
    // Kafka producer
    producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "kafka:9092"})
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Produce a message
    topic := "test-topic"
    producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value:          []byte("Hello from service-a!"),
    }, nil)

    // Kafka consumer
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "kafka:9092",
        "group.id":          "service-a-group",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    consumer.SubscribeTopics([]string{"test-topic"}, nil)

    for {
        msg, err := consumer.ReadMessage(-1)
        if err != nil {
            fmt.Printf("Consumer error: %v (%v)\n", err, msg)
            continue
        }
        fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
    }
}
