package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

var (
	ignoreRetained         bool
	insecure               bool
	ignorePayload          bool
	serverURL              string
	topicFiltersString     string
	clientID               string
	pubTopic               string
	pubMessage             string
	numberMessagesExpected int
)

func main() {
	parseArgs()
	opts := createMqttOptsFromFlags()
	client := connect(opts)

	var wg sync.WaitGroup
	setupSubscriptions(client, &wg)
  publishMessage(client)
	wg.Wait()

  client.Disconnect(1)
	log("done.")
}

func publishMessage(client *mqtt.Client) {
  if pubTopic != "" {
    token := client.Publish(pubTopic, 2, false, pubMessage)
    token.WaitTimeout(10 * time.Second)
    if token.Error() != nil {
      logF("FATAL: Could not publish: %v", token.Error())
      os.Exit(-1)
    }
  } else {
    log("nothing to publish")
  }
}

func parseArgs() {
	flag.BoolVar(&ignoreRetained, "ignore-retained", false, "if TRUE, will only consider live (non-retained) messages")
	flag.BoolVar(&insecure, "insecure", false, "if TRUE, will allow TLS/SSL connections without certificate and hostname validation")
	flag.BoolVar(&ignorePayload, "ignore-payload", false, "if TRUE, will only print a summary line per message")
	flag.StringVar(&serverURL, "url", "tcp://localhost:1883", "the server url to connect to")
	flag.StringVar(&topicFiltersString, "sub", "", "the topic(s) to subscribe to (may be a comma separated list)")
	flag.StringVar(&clientID, "clientid", "", "the mqtt clientid to use (optional)")
	flag.StringVar(&pubTopic, "pub", "", "the topic to publish to (after subscriptions have been setup)")
	flag.StringVar(&pubMessage, "msg", "", "the message to publish on the '-pub' topic")
	flag.IntVar(&numberMessagesExpected, "msg-count", 1, "number of messages to receive before exitting")
	flag.Parse()
}

func createMqttOptsFromFlags() *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(serverURL)
	opts.TLSConfig.InsecureSkipVerify = insecure
	if clientID != "" {
		opts.SetClientID(clientID)
	}
	return opts
}

func connect(opts *mqtt.ClientOptions) *mqtt.Client {
	log("Starting mqtt client")
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Error() != nil {
		logF("FATAL: Could not connect: %v", token.Error())
		os.Exit(-1)
	}
	token.WaitTimeout(10 * time.Second)
	if token.Error() != nil {
		logF("FATAL: Could not connect: %v", token.Error())
		os.Exit(-1)
	}
	if !client.IsConnected() {
		log("FATAL: Not connected after timeout!")
		os.Exit(-1)
	}
	log("connected")
	return client
}

func setupSubscriptions(client *mqtt.Client, wg *sync.WaitGroup) {
	if topicFiltersString != "" {

		wg.Add(numberMessagesExpected)

		topicFilters := strings.Split(topicFiltersString, ",")
		for _, filter := range topicFilters {
			client.Subscribe(filter, 2, func(client *mqtt.Client, msg mqtt.Message) {
				if !msg.Retained() || !ignoreRetained {
					payload := bytes.NewBuffer(msg.Payload()).String()

					if ignorePayload {
						fmt.Printf("r:%v, t:%v, s:%v\n", msg.Retained(), msg.Topic(), len(payload))
					} else {
						logF("r:%v, t:%v, s:%v", msg.Retained(), msg.Topic(), len(payload))
						fmt.Printf("%v\n", payload)
					}
					wg.Done()
				} else {
					log("ignoring retained msg")
				}
			})
		}

		logF("waiting for %v msgs.", numberMessagesExpected)
	} else {
		log("No subscriptions...")
	}
}

func log(msg string) {
	fmt.Fprintln(os.Stderr, msg)
}

func logF(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}
