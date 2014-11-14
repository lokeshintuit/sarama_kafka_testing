package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	clientConfig := sarama.NewClientConfig()
	kafkaBrokers := []string(strings.Split(os.Getenv("KAFKA_BROKERS"), ","))
	client, err := sarama.NewClient("client_id", kafkaBrokers, clientConfig)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	producerConfig := sarama.NewProducerConfig()
	// producerConfig.Compression = sarama.CompressionSnappy
	producerConfig.FlushMsgCount = 1000
	producerConfig.AckSuccesses = true
	producer, err := sarama.NewProducer(client, producerConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		fmt.Println(producer.Close())
	}()
	var msgbyte []byte = []byte(string("[52 255 129 3 1 1 10 77 109 101 77 101 115 115 97 103 101 1 255 130 0 1 2 1 9 68 101 118 105 99 101 77 115 103 1 10 0 1 9 65 99 99 111 117 110 116 73 100 1 12 0 0 0 255 165 255 130 1 255 149 0 0 0 0 1 0 0 0 0 0 0 0 0 84 79 140 62 0 1 27 74 29 166 92 184 0 30 227 152 66 21 85 85 194 244 14 90 0 0 0 0 68 250 0 0 68 250 0 0 0 0 0 0 0 0 0 0 0 0 0 0 23 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 127 255 255 255 0 0 0 0 1 8 117 116 97 103 45 53 53 53 0]"))
	var wg sync.WaitGroup
	nothread, _ := strconv.Atoi(os.Args[2])
	wg.Add(nothread)
	t0 := time.Now()
	for i := 0; i < nothread; i++ {
		go msgSend(producer, msgbyte, &wg)
	}
	wg.Wait()
	t1 := time.Now()
	fmt.Printf("Overall sending took %v to run.\n", t1.Sub(t0))

}

func msgSend(producer *sarama.Producer, msgbyte []byte, wg *sync.WaitGroup) {
	i := 0
	end, _ := strconv.Atoi(os.Args[1])
	t0 := time.Now()
	for j := 0; j < end; j++ {
		select {
		// default:
		case producer.Input() <- &sarama.MessageToSend{Topic: "benchmarktopic", Key: nil, Value: sarama.ByteEncoder(msgbyte)}:
			i++
			if i >= 10000 {
				fmt.Println("batch")
				i = 0
			}
		case err := <-producer.Errors():
			fmt.Println(err)
		case <-producer.Successes():
		}
	}
	t1 := time.Now()
	fmt.Printf("sending took %v to run.\n", t1.Sub(t0))
	wg.Done()
}
