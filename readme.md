# A go MQTT command line client

MQTT client that should be suitable for use in (bash) scripts.

## Usage of gomqtt:
    -clientid string
          the mqtt clientid to use (optional)
    -ignore-payload
          if TRUE, will only print a summary line per message
    -ignore-retained
          if TRUE, will only consider live (non-retained) messages
    -insecure
          if TRUE, will allow TLS/SSL connections without certificate and hostname validation
    -msg string
          the message to publish on the '-pub' topic
    -msg-count int
          number of messages to receive before exitting (default 1)
    -pub string
          the topic to publish to (after subscriptions have been setup)
    -sub string
          the topic(s) to subscribe to (may be a comma separated list)
    -url string
          the server url to connect to (default "tcp://localhost:1883")
