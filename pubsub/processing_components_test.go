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

	nbrReceived, err := ReceiveEventsFromPubsub(os.Getenv("PROJECT_ID"), os.Getenv("OUT_QUEUE_TOPIC_ID"), 0, 4, &pubsubForwardChan, 250, 32)
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


/*
// This was to test a duplicate bug. We might need it again.

const okPayload = `[
   {
      "email":"apa4@banan.com",
      "timestamp":1576683110,
      "smtp-id":"<14c5d75ce93.dfd.64b469@ismtpd-555>",
      "event":"delivered",
      "category":"cat facts",
      "marketing_campaign_id":"supercampaign",
      "sg_event_id":"sg_event_id",
      "sg_message_id":"sg_message_id"
   }
]`


func populateSubscription(topicName string) error {

	forwarderTest.SetEnvVars()

	var nbrWorkers = 10

	chanToWorkers := make(chan int, 2000)
	defer close(chanToWorkers)
	var workerWaitGroup sync.WaitGroup

	for i := 0; i < nbrWorkers; i++ {
		workerWaitGroup.Add(1)

		go func(idx int, workerWaitGroup *sync.WaitGroup) {
			defer workerWaitGroup.Done()

			ctx, client, topic, err := SetupClientAndTopic(os.Getenv("PROJECT_ID"), topicName)
			if nil != client {
				defer client.Close()
			}

			if nil != err {
				fmt.Printf("Failed to setup topic: %v\n", err)
				return
			}

			for {
				val := <- chanToWorkers
				if -1 == val {
					fmt.Printf("asked to die\n")
					break
				}

				structToPush := PubSubElement{
					CompanyID: val,
					ESP: "sg",
					ESPJsonString: okPayload,
					Ts: time.Now().Unix(),
					SafeHash: "safehash",
					Sign: "sign",
				}

				err = PushAndWaitElemToPubsub(ctx, topic, &structToPush)
				if nil != err {
					fmt.Printf("Failed to push element: %v\n", err)
					return
				}
			}

		} (i, &workerWaitGroup)
	}

	for i:=0; i<1200; i++ {
		chanToWorkers <- i+1
	}

	for i:=0; i<nbrWorkers; i++ {
		chanToWorkers <- -1
	}

	workerWaitGroup.Wait()
	return nil
}

func TestPopulateTesting(t *testing.T) {
	populateSubscription("TESTING")
}

func TestPopulateTesting2(t *testing.T) {
	populateSubscription("TESTING2")
}

func TestPopulateTesting3(t *testing.T) {
	populateSubscription("TESTING3")
}

func TestPopulateTesting4(t *testing.T) {
	populateSubscription("TESTING4")
}


var mutexForConsumer sync.Mutex
var dupeTest = make(map[int]bool)


func asyncConsumer(nbrConsumer int, forwardWaitGroup *sync.WaitGroup, pubsubForwardChan *chan *PubSubElement) {
	dupeTest = make(map[int]bool)

	for i := 0; i < nbrConsumer; i++ {
		forwardWaitGroup.Add(1)

		go func(idx int, workerWaitGroup *sync.WaitGroup) {
			defer workerWaitGroup.Done()

			for {
				val := <- *pubsubForwardChan
				if nil == val {
					fmt.Printf("asked to die\n")
					break
				}

				mutexForConsumer.Lock()
				isDupe := dupeTest[val.CompanyID]
				dupeTest[val.CompanyID] = true
				mutexForConsumer.Unlock()

				if isDupe {
					fmt.Printf("!!!!!!!!!! Dupe detected! %d\n", val.CompanyID)
				}
			}

		} (i, forwardWaitGroup)
	}
}

func TestHej(t *testing.T) {
	fmt.Printf("hej\n")
}


func TestToConsumeTheLot(t *testing.T) {

	var BATCHSIZE = 64
	var SUBSCRIPTION = "TESTING2"
	maxNbrMessagesPolled := BATCHSIZE

	forwarderTest.SetEnvVars()

	var nbrConsumer = 10
	var totalReceived = 0

	pubsubForwardChan := make(chan *PubSubElement, 2000)
	defer close(pubsubForwardChan)
	var forwardWaitGroup sync.WaitGroup

	asyncConsumer(nbrConsumer, &forwardWaitGroup, &pubsubForwardChan)


	for {
		// 1. How many elements are there?
		nbrItemsInt64, err := CheckNbrItemsPubsub(os.Getenv("PROJECT_ID"), SUBSCRIPTION)
		if nil != err {
			fmt.Printf("Failed to check nbr items")
		}

		if int64(0) == nbrItemsInt64 {
			break
		}

		// This one starts and takes down the ackQueue
		nbrReceived, err := ReceiveEventsFromPubsub(os.Getenv("PROJECT_ID"), SUBSCRIPTION, 0, maxNbrMessagesPolled, &pubsubForwardChan, 1200, 64)
		if nil != err {
			// Super important too.
			fmt.Printf("forwarder.forward.Forward(%s) failed to receive events: %v\n", "unittest", err)
		}

		totalReceived += nbrReceived

		if 0 == nbrReceived {
			fmt.Printf("Received none.\n")
			break
		}
	}

	fmt.Printf("Total received: %d\n", totalReceived)

	for i:=0; i<nbrConsumer; i++ {
		pubsubForwardChan <- nil
	}

	forwardWaitGroup.Wait()
}
*/




