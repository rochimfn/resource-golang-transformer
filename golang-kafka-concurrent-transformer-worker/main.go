package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var topic = "name_stream"
var destinationTopic = "name_stream_concurrent_transformed"
var kafkaConfigConsumer = kafka.ConfigMap{
	"bootstrap.servers": "localhost:19092",
	"group.id":          "consumer_golang_concurrent",
}
var kafkaConfigProducer = kafka.ConfigMap{
	"bootstrap.servers":            "localhost:19092",
	"acks":                         "all",
	"queue.buffering.max.messages": 1000 * 1000,
}
var produceRetryLimit = 3
var numOfWorker = 4

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

func consumer(quitChan <-chan os.Signal, consumerChan chan<- kafka.Message) {
	c, err := kafka.NewConsumer(&kafkaConfigConsumer)
	handleUnexpectedError(err)
	defer c.Close()
	defer close(consumerChan)

	err = c.SubscribeTopics([]string{topic}, nil)
	handleUnexpectedError(err)
	for {
		select {
		case <-quitChan:
			return
		default:
			msg, err := c.ReadMessage(time.Second)
			if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
				log.Println("timeout")
				time.Sleep(1 * time.Second)
				continue
			}
			handleUnexpectedError(err)

			consumerChan <- *msg
		}
	}
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

func worker(consumerChan <-chan kafka.Message) {
	p, err := kafka.NewProducer(&kafkaConfigProducer)
	handleUnexpectedError(err)
	go ack(p)
	defer p.Close()
	defer flushAll(p)

	for message := range consumerChan {
		event := NameEvent{}
		err := json.Unmarshal(message.Value, &event)
		handleUnexpectedError(err)

		event.Hash = fmt.Sprintf("%x", md5.Sum(message.Value))
		message.Value, err = json.Marshal(event)
		handleUnexpectedError(err)

		numRetry := 0
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &destinationTopic, Partition: kafka.PartitionAny},
			Key:            message.Key, Value: message.Value,
		}
		for err = p.Produce(msg, nil); err != nil && numRetry < produceRetryLimit; {
			numRetry += 1
			log.Printf("error produce %s retrying after flush\n", err)
			flushAll(p)
		}
	}
}

func main() {
	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, syscall.SIGTERM)

	consumerChan := make(chan kafka.Message, 1000*1000)
	wg := sync.WaitGroup{}
	wg.Add(1 + numOfWorker)
	go func() {
		consumer(quitChan, consumerChan)
		wg.Done()
	}()

	for i := 0; i < numOfWorker; i++ {
		go func() {
			worker(consumerChan)
			wg.Done()
		}()
	}

	wg.Wait()
}
