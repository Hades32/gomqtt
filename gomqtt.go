package main

import (
	"fmt"
	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"net/url"
	"os"
	"time"
  "bytes"
  "flag"
)

var (
  ignoreRetained bool
  topic string
  numberMessagesExpected int
  urls []*url.URL
)

func parseArgs() {
  flag.StringVar(&topic, "topic", "", "the topic to subscribe to")
  flag.BoolVar(&ignoreRetained, "ignore-retained", false, "if TRUE, will only consider live (non-retained) messages")
  flag.IntVar(&numberMessagesExpected, "msg-count", 1, "number of messages to receive before exitting")
  flag.Parse()  
  
  if topic=="" {
    fmt.Println("Bad topic")
    os.Exit(-1)
  }
  
  url1, err := url.Parse("ssl://telerent-int.car2go.com:18883")
  if err!=nil {
    fmt.Println("Bad url")
    os.Exit(-1)
  }
  urls = []*url.URL { url1 }  
}

func main() {
  parseArgs()
  
	log("Starting mqtt client")
  opts := mqtt.NewClientOptions()
  opts.Servers = urls 
  client := mqtt.NewClient(opts)
  token := client.Connect()
  token.WaitTimeout(10*time.Second)
  if client.IsConnected() == false {
    log("Not connected after timeout!")
    os.Exit(-1)
  }
  log("connected")
  
  received := make(chan bool)  
  
  client.Subscribe(topic, 2, func (client *mqtt.Client, msg mqtt.Message){
    if !msg.Retained() || !ignoreRetained {
      payload := bytes.NewBuffer(msg.Payload()).String()
      fmt.Printf("r:%v, t:%v, m:%v\n", msg.Retained(), msg.Topic(), payload)
      received <- true
    } else {
      log("ignoring retained msg")
    }
  })
  
  logF("waiting for %v msgs.\n", numberMessagesExpected)
  for i := 0; i < numberMessagesExpected; i++ {
    <- received
  }
  
  log("done.")
}

func log(msg string) {
  fmt.Fprintln(os.Stderr, msg)
}

func logF(format string, a ...interface{}) {
  fmt.Fprintf(os.Stderr, format, a...)
} 
