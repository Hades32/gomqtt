package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/url"
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
	numberMessagesExpected int
	urls                   []*url.URL
)

func parseArgs() {
	flag.StringVar(&serverURL, "url", "tcp://localhost:1883", "the server url to connect to")
	flag.StringVar(&topicFiltersString, "sub", "", "the topic(s) to subscribe to (may be a comma separated list)")
	flag.BoolVar(&ignoreRetained, "ignore-retained", false, "if TRUE, will only consider live (non-retained) messages")
	flag.BoolVar(&insecure, "insecure", false, "if TRUE, will allow TLS/SSL connections without certificate and hostname validation")
	flag.BoolVar(&ignorePayload, "ignore-payload", false, "if TRUE, will only print a summary line per message")
	flag.IntVar(&numberMessagesExpected, "msg-count", 1, "number of messages to receive before exitting")
	flag.Parse()

	if topicFiltersString == "" {
		fmt.Println("Bad topic")
    flag.Usage()
		os.Exit(-1)
	}

	url1, err := url.Parse(serverURL)
	if err != nil {
		fmt.Println("Bad url")
    flag.Usage()
		os.Exit(-1)
	}
	urls = []*url.URL{url1}
}

func main() {
	parseArgs()

	log("Starting mqtt client")
	opts := mqtt.NewClientOptions()
	opts.Servers = urls
	opts.TLSConfig.InsecureSkipVerify = true
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

	var wg sync.WaitGroup

  wg.Add(numberMessagesExpected)
  
  topicFilters := strings.Split(topicFiltersString, ",")
  for _,filter := range topicFilters {
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
	wg.Wait()

	log("done.")
}

func log(msg string) {
	fmt.Fprintln(os.Stderr, msg)
}

func logF(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}
