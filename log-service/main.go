package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LogMessage struct {
    Service   string `json:"service"`
    Function  string `json:"function"`
    Level     string `json:"level"`
    Message   string `json:"message"`
    Timestamp string `json:"timestamp"`
}

func main() {
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "kafka:9092",
        "group.id":          "log-service-group",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    consumer.SubscribeTopics([]string{"log-topic"}, nil)

    for {
        msg, err := consumer.ReadMessage(-1)
        if err != nil {
            fmt.Printf("Consumer error: %v (%v)\n", err, msg)
            continue
        }

        var logMsg LogMessage
        err = json.Unmarshal(msg.Value, &logMsg)
        if err != nil {
            fmt.Printf("Failed to unmarshal log message: %v\n", err)
            continue
        }

        fmt.Printf("[%s] [%s] [%s] %s: %s\n", logMsg.Timestamp, logMsg.Service, logMsg.Level, logMsg.Function, logMsg.Message)
    }
}
