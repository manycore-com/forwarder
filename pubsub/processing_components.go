package forwarderpubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type PubSubElement struct {
	CompanyID     int
	ESP           string
	ESPJsonString string
	Ts            int64
	SafeHash      string
	Sign          string  // Sign hash where applicable
	Dest          string  // added here in fanout.
}

// This decouples msg.Ack() from the other flow.
func asyncAckMessages(devprod string, ackQueue *chan *pubsub.Message, ackWaitGroup *sync.WaitGroup, nbrAckWorker int) {

	for i := 0; i < nbrAckWorker; i++ {

		ackWaitGroup.Add(1)

		go func(idx int, ackWaitGroup *sync.WaitGroup) {

			defer ackWaitGroup.Done()

			for {

				msg := <- *ackQueue
				if nil == msg {
					//fmt.Printf("asyncAckMessages(%s,%d) done.\n", devprod, idx)
					break
				} else {
					//fmt.Printf("asyncAckMessages(%s,%d) ack message: %v\n", devprod, idx, *msg)
					msg.Ack()
				}
			}
		} (i, ackWaitGroup)
	}
}

// I don't know how to "give me 1000 objects in max 60s". It seems to always wait for 60s. So I do a small timeout
// and increase it if there are actual messages.
func receiveEventsFromPubsubPoller(
	devprod string,
	subscription *pubsub.Subscription,
	pubsubForwardChan *chan *PubSubElement,
	timeoutSeconds int,
	minAgeSec int,
	nbrReceivedBefore int,
	maxPolled int,
	ackQueue *chan *pubsub.Message) (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds) * time.Second)
	defer cancel()

	var mu sync.Mutex
	received := 0

	minAgeSecF64 := float64(minAgeSec)
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		received++

		var elem PubSubElement
		err := json.Unmarshal(msg.Data, &elem)
		if nil == err {
			if AgeInSecMessage(msg) < minAgeSecF64 {
				cancel()
				msg.Nack()
			} else {
				fmt.Printf("forwarder.pubsub.receiveEventsFromPubsubPoller() age:%v, Message: %#v\n", AgeInSecMessage(msg), elem)
				*ackQueue <- msg
				*pubsubForwardChan <- &elem
			}
		} else {
			fmt.Printf("receiveEventsFromPubsubPoller(%s): Error: failed to Unmarshal: %v\n", devprod, err)
		}

		if (received + nbrReceivedBefore) >= maxPolled {
			cancel()
		}
	})

	return received, err
}

// ReceiveEventsFromPubsub is synchronous in it's nature. It will lock up main thread. Do not call until main pipeline
// is setup or it might deadlock if the buffered chan gets full.
func ReceiveEventsFromPubsub(
	devprod string,
	projectId string,
	subscriptionId string,
	minAgeSecs int,
	nbrAckWorker int,
	maxPollPerRun int,
	pubsubForwardChan *chan *PubSubElement) (int, error) {

	ctx := context.Background()
	client, clientErr := pubsub.NewClient(ctx, projectId)
	if clientErr != nil {
		return 0, clientErr
	}
	if nil != client {
		defer client.Close()
	}

	subscription := client.Subscription(subscriptionId)
	subscription.ReceiveSettings.Synchronous = true
	subscription.ReceiveSettings.MaxOutstandingMessages = nbrAckWorker  // There are less thoughts about this than you think

	ackQueue := make(chan *pubsub.Message, 2000)
	defer close(ackQueue)
	var ackWaitGroup sync.WaitGroup

	asyncAckMessages(devprod, &ackQueue, &ackWaitGroup, nbrAckWorker)

	var nbrReceivedTotal = 0
	var err error = nil
	var pollMax = 1000

	nbrItemsInt64, checkErr := CheckNbrItemsPubsub(projectId, subscriptionId)
	if checkErr == nil {
		fmt.Printf("forwarder.pubsub.ReceiveEventsFromPubsub(%s): queue size: %v\n", devprod, nbrItemsInt64)
		pollMax = int(nbrItemsInt64)
		if pollMax > maxPollPerRun {
			pollMax = maxPollPerRun
		}
	} else {
		fmt.Printf("forwarder.pubsub.ReceiveEventsFromPubsub(%s): Failed to check queue size for %s: err=%v\n", devprod, subscriptionId, checkErr)
		pollMax = maxPollPerRun
	}

	if 0 == pollMax {
		return 0, nil
	}

	var timeout int
	if pollMax > 500 {
		timeout = 60
	} else if timeout > 100 {
		timeout = 30
	} else {
		timeout = 15
	}

	nbrReceivedTotal, err = receiveEventsFromPubsubPoller(devprod, subscription, pubsubForwardChan, timeout, minAgeSecs, 0, pollMax, &ackQueue)

	for i:=0; i< nbrAckWorker; i++ {
		ackQueue <- nil
	}

	fmt.Printf("receiveEventsFromPubsub(%s): waiting for ackWaitGroup...\n", devprod)
	ackWaitGroup.Wait()

	fmt.Printf("receiveEventsFromPubsub(%s): done. NbrReceived=%d, err=%v\n", devprod, nbrReceivedTotal, err)

	return nbrReceivedTotal, nil
}
