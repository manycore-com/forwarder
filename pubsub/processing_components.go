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

// I don't know how to "give me 1000 objects in max 60s". It seems to always wait for 60s. So I do a small timeout
// and increase it if there are actual messages.
func receiveEventsFromPubsubPoller(
	devprod string,
	projectId string,
	subscriptionId string,
	pubsubForwardChan *chan *PubSubElement,
	timeoutSeconds int,
	minAgeSec int,
	nbrReceivedBefore int,
	maxPolled int,
	maxPubsubQueueIdleMs int,
	maxOutstandingMessages int) (int, error) {


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
	subscription.ReceiveSettings.MaxOutstandingMessages = maxOutstandingMessages
	subscription.ReceiveSettings.MaxOutstandingBytes = 20000000

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds) * time.Second)
	defer cancel()

	var mu sync.Mutex
	received := 0

	var runTick = true
	var startMs = time.Now().UnixNano() / 1000000
	// 3000ms was too low for local machine. 8000ms was enough. It's reset after first received message.
	var lastAtMs = startMs + 5000
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			mu.Lock()
			var copyOfRunTick = runTick
			var copyOfLastAtMs = lastAtMs
			mu.Unlock()

			if !copyOfRunTick {
				return
			}

			var rightNow = time.Now().UnixNano() / 1000000
			if (int64(maxPubsubQueueIdleMs) + copyOfLastAtMs) < rightNow {
				fmt.Printf("forwarder.pubsub.receiveEventsFromPubsubPoller.func() Killing Receive due to %dms inactivity.\n", maxPubsubQueueIdleMs)
				mu.Lock()
				runTick = false
				mu.Unlock()
				cancel()
				return
			}
		}
	} ()

	minAgeSecF64 := float64(minAgeSec)
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		var runTickCopy = runTick
		var receivedCopy = received
		mu.Unlock()

		if ! runTickCopy {
			fmt.Printf("forwarder.pubsub.receiveEventsFromPubsubPoller() we are canceled.\n")
			defer msg.Nack()
			return
		}

		var elem PubSubElement
		err := json.Unmarshal(msg.Data, &elem)
		if nil == err {
			if AgeInSecMessage(msg) < minAgeSecF64 {
				mu.Lock()
				runTick = false
				mu.Unlock()

				msg.Nack()
				cancel()
			} else {
				fmt.Printf("forwarder.pubsub.receiveEventsFromPubsubPoller() Ack age:%v, Message: %#v\n", AgeInSecMessage(msg), elem)
				msg.Ack()

				mu.Lock()
				received++
				receivedCopy = received
				lastAtMs = time.Now().UnixNano() / 1000000
				mu.Unlock()

				*pubsubForwardChan <- &elem
			}
		} else {
			fmt.Printf("receiveEventsFromPubsubPoller(%s): Error: failed to Unmarshal: %v\n", devprod, err)
			msg.Ack()  // Valid or not, Ack to get rid of it
		}

		if (receivedCopy + nbrReceivedBefore) >= maxPolled {
			cancel()

			mu.Lock()
			runTick = false
			mu.Unlock()
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
	maxPollPerRun int,
	pubsubForwardChan *chan *PubSubElement,
    maxPubsubQueueIdleMs int,
	maxOutstandingMessages int) (int, error) {

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

	var nbrReceived int = 0

	var timeout int = 30
	nbrReceived, err = receiveEventsFromPubsubPoller(devprod, projectId, subscriptionId, pubsubForwardChan, timeout, minAgeSecs, 0, pollMax - nbrReceivedTotal, maxPubsubQueueIdleMs, maxOutstandingMessages)

	nbrReceivedTotal += nbrReceived

	/*
	if pollMax > 5 {
		if (pollMax * 2 / 3 ) > nbrReceivedTotal {

			ctx := context.Background()
			client, clientErr := pubsub.NewClient(ctx, projectId)
			if clientErr != nil {
				return 0, clientErr
			}
			if nil != client {
				defer client.Close()
			}

			fmt.Printf("forwarder.pubsub.ReceiveEventsFromPubsub(%s): 2nd run: %d > %d\n", devprod, (pollMax * 2 / 3 ), nbrReceivedTotal)

			timeout = 15
			nbrReceived, err = receiveEventsFromPubsubPoller(devprod, projectId, subscriptionId, pubsubForwardChan, timeout, minAgeSecs, nbrReceivedTotal, pollMax - nbrReceivedTotal, maxPubsubQueueIdleMs, maxOutstandingMessages)
			nbrReceivedTotal += nbrReceived
		}
	}
	 */

	fmt.Printf("receiveEventsFromPubsub(%s): done. NbrReceived=%d, err=%v\n", devprod, nbrReceivedTotal, err)

	return nbrReceivedTotal, nil
}
