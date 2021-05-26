package forwarderpubsub

import (
	"fmt"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"os"
	"testing"
)

func TestReceiveEventsFromPubsub(t *testing.T) {

	forwarderTest.SetEnvVars()

	pubsubForwardChan := make(chan *PubSubElement, 2000)
	defer close(pubsubForwardChan)
	//var forwardWaitGroup sync.WaitGroup

	nbrReceived, err := ReceiveEventsFromPubsub(os.Getenv("DEV_OR_PROD"), os.Getenv("PROJECT_ID"), os.Getenv("OUT_QUEUE_TOPIC_ID"), 0, 4, 4, &pubsubForwardChan)
	if nil != err {
		fmt.Printf("forwarder.pubsub.TestReceiveEventsFromPubsub(): Failed to poll: %v\n", err)
	} else {
		fmt.Printf("forwarder.pubsub.TestReceiveEventsFromPubsub(): got nbr elements: %d\n", nbrReceived)
	}

}
