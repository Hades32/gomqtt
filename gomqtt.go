package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"compress/gzip"
	"io/ioutil"

	"math"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	debugEnabled           bool
	ignoreRetained         bool
	insecure               bool
	ignorePayload          bool
	serverURL              string
	topicFiltersString     string
	clientID               string
	username               string
	password               string
	pubTopic               string
	pubMessage             string
	numberMessagesExpected int
	qos                    byte
	logger                 = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)
)

func main() {
	parseArgs()
	opts := createMqttOptsFromFlags()
	client := connect(opts)

	var wg sync.WaitGroup
	setupSubscriptions(client, &wg)
	publishMessage(client)
	wg.Wait()

	info("done.")
}

func parseArgs() {
	var qosInt int
	flag.BoolVar(&debugEnabled, "debug", false, "if set, display debug messages")
	flag.BoolVar(&ignoreRetained, "ignore-retained", false, "if set, will only consider live (non-retained) messages")
	flag.BoolVar(&insecure, "insecure", false, "if set, will allow TLS/SSL connections without certificate and hostname validation")
	flag.BoolVar(&ignorePayload, "ignore-payload", false, "if set, will only print a summary line per message")
	flag.StringVar(&serverURL, "url", "tcp://localhost:1883", "the server url to connect to")
	flag.StringVar(&topicFiltersString, "sub", "", "the topic(s) to subscribe to (may be a comma separated list)")
	flag.StringVar(&clientID, "clientid", "", "the mqtt clientid to use (optional)")
	flag.StringVar(&username, "username", "", "the mqtt username to use (optional)")
	flag.StringVar(&password, "password", "", "the mqtt password to use (optional)")
	flag.StringVar(&pubTopic, "pub", "", "the topic to publish to (after subscriptions have been setup)")
	flag.StringVar(&pubMessage, "msg", "", "the message to publish on the '-pub' topic")
	flag.IntVar(&numberMessagesExpected, "msg-count", math.MaxInt32, "number of messages to receive before exitting")
	flag.IntVar(&qosInt, "qos", 0, "QoS for publishes and subscriptions")
	flag.Parse()
	qos = byte(qosInt)

	if debugEnabled {
		mqtt.DEBUG = logger
		mqtt.WARN = logger
		mqtt.ERROR = logger
		mqtt.CRITICAL = logger
	}
}

func createMqttOptsFromFlags() *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(serverURL)
	opts.TLSConfig.InsecureSkipVerify = insecure
	if clientID != "" {
		opts.SetClientID(clientID)
	}
	opts.SetUsername(username)
	opts.SetPassword(password)
	return opts
}

func connect(opts *mqtt.ClientOptions) mqtt.Client {
	info("Starting mqtt client")
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Error() != nil {
		infoF("FATAL: Could not connect: %v", token.Error())
		os.Exit(-1)
	}
	token.WaitTimeout(10 * time.Second)
	if token.Error() != nil {
		infoF("FATAL: Could not connect: %v", token.Error())
		os.Exit(-1)
	}
	if !client.IsConnected() {
		info("FATAL: Not connected after timeout!")
		os.Exit(-1)
	}
	info("connected")
	return client
}

func setupSubscriptions(client mqtt.Client, wg *sync.WaitGroup) {
	if topicFiltersString != "" {

		wg.Add(numberMessagesExpected)

		topicFilters := strings.Split(topicFiltersString, ",")
		for _, filter := range topicFilters {
			client.Subscribe(filter, qos, func(client mqtt.Client, msg mqtt.Message) {
				if !msg.Retained() || !ignoreRetained {
					buffer := bytes.NewBuffer(msg.Payload())
					if strings.HasSuffix(msg.Topic(), ".GZ") || strings.HasSuffix(msg.Topic(), ".gz") {
						reader, _ := gzip.NewReader(buffer)
						readBytes, _ := ioutil.ReadAll(reader)
						buffer = bytes.NewBuffer(readBytes)
					}
					payload := buffer.String()

					if ignorePayload {
						fmt.Printf("r:%v, t:%v, s:%v\n", msg.Retained(), msg.Topic(), len(payload))
					} else {
						infoF("r:%v, t:%v, s:%v", msg.Retained(), msg.Topic(), len(payload))
						fmt.Printf("%v\n", payload)
					}
					wg.Done()
				} else {
					info("ignoring retained msg")
				}
			})
		}

		infoF("waiting for %v msgs.", numberMessagesExpected)
	} else {
		info("No subscriptions...")
	}
}

func publishMessage(client mqtt.Client) {
	if pubTopic != "" {
		token := client.Publish(pubTopic, qos, false, pubMessage)
		token.WaitTimeout(10 * time.Second)
		if token.Error() != nil {
			infoF("FATAL: Could not publish: %v", token.Error())
			os.Exit(-1)
		}
		info("published message succesfully")
	} else {
		info("nothing to publish")
	}
}

func info(msg string) {
	logger.Println(msg)
}

func infoF(format string, a ...interface{}) {
	logger.Printf(format+"\n", a...)
}
