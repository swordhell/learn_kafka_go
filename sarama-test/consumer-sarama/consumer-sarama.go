package main

import (
	"context"
	"fmt"
	"os"
	"sync"

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

		if consumer, err := sarama.NewConsumerFromClient(client); err != nil {
			fmt.Println("subscribeTopic NewConsumerFromClient fail err ", err.Error(), " addr ", addr)
			return
		} else {

			if partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest); err != nil {
				consumer.Close()
				fmt.Println("subscribeTopic ConsumePartition fail ", err.Error(), " addr ", addr)
				return
			} else {
				go consumeLoop(partition)
				fmt.Println("subscribeTopic ConsumePartition success addr ", addr)
			}
		}
	}

	c := make(chan os.Signal, 1)
	<-c

	ctxFun()
	wg.Wait()
}

func consumeLoop(partition sarama.PartitionConsumer) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case msg, ok := <-partition.Messages():
			if !ok {
				fmt.Println("consumeLoop read messages fail Partition.Messages error addr ")
				return
			}
			strMsg := string(msg.Value)
			fmt.Println("consumeLoop value ", strMsg)
		case err, ok := <-partition.Errors():
			if ok {
				fmt.Println("consumeLoop read messages fail error ", err.Error())
			}
		case <-ctx.Done():
			fmt.Println("consumeLoop exit")
			return
		}
	}
}
