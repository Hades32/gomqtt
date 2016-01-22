package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

var (
	ignoreRetained         bool
	serverURL              string
	topicFilter            string
	numberMessagesExpected int
	urls                   []*url.URL
)

func parseArgs() {
	flag.StringVar(&serverURL, "url", "tcp://localhost:1883", "the server url to connect to")
	flag.StringVar(&topicFilter, "topic", "", "the topic to subscribe to")
	flag.BoolVar(&ignoreRetained, "ignore-retained", false, "if TRUE, will only consider live (non-retained) messages")
	flag.IntVar(&numberMessagesExpected, "msg-count", 1, "number of messages to receive before exitting")
	flag.Parse()

	if topicFilter == "" {
		fmt.Println("Bad topic")
		os.Exit(-1)
	}

	url1, err := url.Parse(serverURL)
	if err != nil {
		fmt.Println("Bad url")
		os.Exit(-1)
	}
	urls = []*url.URL{url1}
}

func main() {
	parseArgs()

	log("Starting mqtt client")
	opts := mqtt.NewClientOptions()
	opts.Servers = urls
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Error() != nil {
		logF("Could not connect: %v", token.Error())
		os.Exit(-1)
	}
	token.WaitTimeout(10 * time.Second)
	if token.Error() != nil {
		logF("Could not connect: %v", token.Error())
		os.Exit(-1)
	}
	if !client.IsConnected() {
		log("Not connected after timeout!")
		os.Exit(-1)
	}
	log("connected")

	received := make(chan bool)

	client.Subscribe(topicFilter, 2, func(client *mqtt.Client, msg mqtt.Message) {
		if !msg.Retained() || !ignoreRetained {
			payload := bytes.NewBuffer(msg.Payload()).String()
			logF("r:%v, t:%v, s:%v", msg.Retained(), msg.Topic(), len(payload))
			fmt.Printf("%v\n", payload)
			received <- true
		} else {
			log("ignoring retained msg")
		}
	})

	logF("waiting for %v msgs.", numberMessagesExpected)
	for i := 0; i < numberMessagesExpected; i++ {
		<-received
	}

	log("done.")
}

func log(msg string) {
	fmt.Fprintln(os.Stderr, msg)
}

func logF(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}
