package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	ctx    context.Context
	ctxFun context.CancelFunc
	mq     chan string
	wg     sync.WaitGroup
)

const topic string = "test-topic"

func main() {

	ctx, ctxFun = context.WithCancel(context.Background())
	mq = make(chan string, 10)
	addr := []string{"127.0.0.1:9092"}

	config := sarama.NewConfig()

	if client, err := sarama.NewClient(addr, config); err != nil {
		fmt.Println("initProducer NewClient error ", err.Error())
		return
	} else {
		defer client.Close()
		if producer, err := sarama.NewAsyncProducerFromClient(client); err != nil {
			fmt.Println("initProducer NewAsyncProducerFromClient error ", err.Error())
			return
		} else {

			defer producer.Close()
			go sendLoop(producer)
			fmt.Println("initProducer success")
		}
	}

	tick := time.NewTicker(time.Second * 2)
	go func() {
		for {
			select {
			case <-tick.C:
				mq <- "test message"
			case <-ctx.Done():
				return
			}
		}
	}()

	c := make(chan os.Signal, 1)
	<-c
}

func sendLoop(producer sarama.AsyncProducer) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case msg, ok := <-mq:
			if !ok {
				return
			}
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Partition: 0, Key: nil, Value: sarama.StringEncoder(msg)}
		case err, ok := <-producer.Errors():
			if ok {
				fmt.Println("sendLoop send messages fail error ", err.Error())
			}
		case <-ctx.Done():
			fmt.Println("sendLoop exit ")
			return
		}
	}
}
