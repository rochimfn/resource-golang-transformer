package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var topic = "name_stream"
var destinationTopic = "name_stream_serial_transformed"
var kafkaConfigConsumer = kafka.ConfigMap{
	"bootstrap.servers": "localhost:19092",
	"group.id":          "consumer_golang_serial",
}
var kafkaConfigProducer = kafka.ConfigMap{
	"bootstrap.servers": "localhost:19092",
	"acks":              "all",
}
var produceRetryLimit = 3

type NameEvent struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Hash      string `json:"hash"`
}

func handleUnexpectedError(err error) {
	if err != nil {
		log.Fatalf("unexpected error occured: %s \n", err)
	}
}

func hashMessage(key []byte, value []byte) ([]byte, []byte) {
	event := NameEvent{}
	err := json.Unmarshal(value, &event)
	handleUnexpectedError(err)

	event.Hash = fmt.Sprintf("%x", md5.Sum(value))
	value, err = json.Marshal(event)
	handleUnexpectedError(err)

	return key, value
}

func ack(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message: %s: %s\n", string(ev.Value), ev.TopicPartition.Error)
			} else {
				log.Printf("Successfully produced message {%s} into {%s}[{%d}]@{%s}\n",
					string(ev.Value), *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset.String())
			}
		}
	}
}

func flushAll(p *kafka.Producer) {
	for unlfushed := p.Flush(30 * 1000); unlfushed > 0; {
	}
}

func consumeLoop(quit <-chan os.Signal) {
	c, err := kafka.NewConsumer(&kafkaConfigConsumer)
	handleUnexpectedError(err)
	defer c.Close()

	p, err := kafka.NewProducer(&kafkaConfigProducer)
	handleUnexpectedError(err)
	go ack(p)
	defer p.Close()
	defer flushAll(p)

	err = c.SubscribeTopics([]string{topic}, nil)
	handleUnexpectedError(err)
	for {
		select {
		case <-quit:
			return
		default:
			msg, err := c.ReadMessage(time.Second)
			if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
				time.Sleep(1 * time.Second)
				continue
			}
			handleUnexpectedError(err)

			key, value := hashMessage(msg.Key, msg.Value)

			numRetry := 0
			message := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &destinationTopic, Partition: kafka.PartitionAny},
				Key:            key, Value: value,
			}
			for err = p.Produce(message, nil); err != nil && numRetry < produceRetryLimit; {
				numRetry += 1
				log.Printf("error produce %s retrying after flush\n", err)
				flushAll(p)
			}
		}
	}

}

func main() {
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, syscall.SIGTERM)

	consumeLoop(quitChan)
}
