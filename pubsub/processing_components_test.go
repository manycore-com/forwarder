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

func TestMisc(t *testing.T) {
	var maxPollPerRun int = 64
	var pollMax = 1000

	var nbrItemsInt64 int64 = 1
	pollMax = int(nbrItemsInt64)

	if pollMax > maxPollPerRun {
		pollMax = maxPollPerRun
	}

	var timeout int
	if pollMax > 500 {
		timeout = 60
	} else if timeout > 100 {
		timeout = 30
	} else {
		timeout = 15
	}

	fmt.Printf("timeout: %v, pollMax:%v\n", timeout, pollMax)

}