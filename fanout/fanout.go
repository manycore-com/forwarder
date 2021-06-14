package fanout

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"os"
	"strconv"
	"sync"
)

func CalculateSafeHashFromSecret(secret string) string {
	var calculatedHash = fmt.Sprintf("%x", sha256.Sum256([]byte(secret)))
	hashHead := calculatedHash[0:32]
	return hashHead
}

var projectId = ""
var inSubscriptionId = ""
var outQueueTopicId = ""
var minAge = -1  // No delay!
var devprod = ""  // Optional: We use dev for development, devprod for live test, prod for live
var nbrAckWorkers = 32
var nbrPublishWorkers = 32
var maxNbrMessagesPolled = 64
var maxPubsubQueueIdleMs = 250
var maxOutstandingMessages = 32
func env() error {
	projectId = os.Getenv("PROJECT_ID")
	inSubscriptionId = os.Getenv("IN_SUBSCRIPTION_ID")
	outQueueTopicId = os.Getenv("OUT_QUEUE_TOPIC_ID")

	if projectId == "" {
		return fmt.Errorf("missing PROJECT_ID environment variable")
	}

	if outQueueTopicId == "" {
		return fmt.Errorf("missing OUT_QUEUE_TOPIC_ID environment variable")
	}

	devprod = os.Getenv("DEV_OR_PROD")

	var err error
	// Optional: How many go threads should send ACK on received messages?
	if "" != os.Getenv("NBR_ACK_WORKER") {
		nbrAckWorkers, err = strconv.Atoi(os.Getenv("NBR_ACK_WORKER"))
		if nil != err {
			return fmt.Errorf("failed to parse integer NBR_ACK_WORKER: %v", err)
		}

		if 1 > nbrAckWorkers {
			return fmt.Errorf("optional NBR_ACK_WORKER environent variable must be at least 1: %v", nbrAckWorkers)
		}

		if 1000 < nbrAckWorkers {
			return fmt.Errorf("optional NBR_ACK_WORKER environent should not be over 1000: %v", nbrAckWorkers)
		}
	}

	// Optional: How many go threads should send ACK on received messages?
	if "" != os.Getenv("NBR_PUBLISH_WORKER") {
		nbrPublishWorkers, err = strconv.Atoi(os.Getenv("NBR_PUBLISH_WORKER"))
		if nil != err {
			return fmt.Errorf("failed to parse integer NBR_PUBLISH_WORKER: %v", err)
		}

		if 1 > nbrPublishWorkers {
			return fmt.Errorf("optional NBR_PUBLISH_WORKER environent variable must be at least 1: %v", nbrPublishWorkers)
		}

		if 1000 < nbrPublishWorkers {
			return fmt.Errorf("optional NBR_PUBLISH_WORKER environent should not be over 1000: %v", nbrPublishWorkers)
		}
	}

	// Optional: How many go threads should send ACK on received messages?
	if "" != os.Getenv("MAX_NBR_MESSAGES_POLLED") {
		maxNbrMessagesPolled, err = strconv.Atoi(os.Getenv("MAX_NBR_MESSAGES_POLLED"))
		if nil != err {
			return fmt.Errorf("failed to parse integer MAX_NBR_MESSAGES_POLLED: %v", err)
		}

		if 1 > maxNbrMessagesPolled {
			return fmt.Errorf("optional MAX_NBR_MESSAGES_POLLED environent variable must be at least 1: %v", maxNbrMessagesPolled)
		}

		if 1000 < maxNbrMessagesPolled {
			return fmt.Errorf("optional MAX_NBR_MESSAGES_POLLED environent variable must be max 1000: %v", maxNbrMessagesPolled)
		}
	}

	if "" != os.Getenv("MAX_PUBSUB_QUEUE_IDLE_MS") {
		maxPubsubQueueIdleMs, err = strconv.Atoi(os.Getenv("MAX_PUBSUB_QUEUE_IDLE_MS"))
		if nil != err {
			return fmt.Errorf("failed to parse integer MAX_PUBSUB_QUEUE_IDLE_MS: %v", err)
		}

		if 100 > maxPubsubQueueIdleMs {
			return fmt.Errorf("optional MAX_PUBSUB_QUEUE_IDLE_MS environent variable must be at least 100: %v\n", maxPubsubQueueIdleMs)
		}

		if 10000 < maxPubsubQueueIdleMs {
			return fmt.Errorf("optional MAX_PUBSUB_QUEUE_IDLE_MS environent variable must be max 10000: %v\n", maxPubsubQueueIdleMs)
		}
	}

	if "" != os.Getenv("MAX_OUTSTANDING_MESSAGES") {
		maxOutstandingMessages, err := strconv.Atoi(os.Getenv("MAX_OUTSTANDING_MESSAGES"))
		if nil != err {
			return fmt.Errorf("failed to parse integer MAX_OUTSTANDING_MESSAGES: %v", err)
		}

		if 1 > maxOutstandingMessages {
			return fmt.Errorf("optional MAX_OUTSTANDING_MESSAGES environent variable must be at least 1: %v\n", maxOutstandingMessages)
		}

		if 100 < maxOutstandingMessages {
			return fmt.Errorf("optional MAX_OUTSTANDING_MESSAGES environent variable must be max 100: %v\n", maxOutstandingMessages)
		}
	}

	return nil
}

func asyncFanout(pubsubForwardChan *chan *forwarderPubsub.PubSubElement, forwardWaitGroup *sync.WaitGroup) {

	for i := 0; i < nbrPublishWorkers; i++ {
		forwardWaitGroup.Add(1)

		go func(idx int, forwardWaitGroup *sync.WaitGroup) {
			defer forwardWaitGroup.Done()

			ctx, client, outQueueTopic, err := forwarderPubsub.SetupClientAndTopic(projectId, outQueueTopicId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				fmt.Printf("forwarder.fanout.asyncFanout(%s,%d): Critical Error: Failed to instantiate Topic Client: %v\n", devprod, idx, err)
				return
			}

			ignoreCompany := map[int]bool{}
			hasSetMessage := map[int]bool{}
			for {
				elem := <- *pubsubForwardChan
				if nil == elem {
					//fmt.Printf("forwarder.fanout.asyncFanout(%s,%d): done.\n", devprod, idx)
					break
				}

				// First, determine where to send it.
				if ignoreCompany[elem.CompanyID] {
					continue
				}

				ci, err := forwarderDb.GetUserData(elem.CompanyID)
				if nil != err {
					fmt.Printf("forwarder.fanout.asyncFanout() failed to get user data: %v\n", err)
					ignoreCompany[elem.CompanyID] = true
					continue
				}

				hashHead := CalculateSafeHashFromSecret(ci.Secret)

				if hashHead != elem.SafeHash {
					fmt.Printf("forwarder.fanout.asyncFanout(): Safe hash is wrong. companyId=%d wrongHash=%s\n", elem.CompanyID, elem.SafeHash)
					forwarderStats.AddLost(elem.CompanyID)
					continue
				}

				if 0 == len(ci.ForwardUrl) {
					ignoreCompany[elem.CompanyID] = true
					continue
				}

				forwarderStats.AddReceivedAtH(elem.CompanyID)

				for _, forwardUrl := range ci.ForwardUrl {
					elem.Dest = forwardUrl

					if ! hasSetMessage[elem.CompanyID] {
						hasSetMessage[elem.CompanyID] = true
						payload, err := json.Marshal(elem)
						if nil == err {
							forwarderStats.AddExample(elem.CompanyID, string(payload))
						} else {
							fmt.Printf("forwarder.fanout.asyncFanout(%s,%d): failed to Marshal element. companyId=%d err=%v\n", devprod, idx, elem.CompanyID, err)
						}
					}

					err = forwarderPubsub.PushElemToPubsub(ctx, outQueueTopic, elem)
					if err != nil {
						fmt.Printf("forwarder.fanout.asyncFanout(%s,%d): Error: Failed to send to %s pubsub: %v\n", devprod, idx, outQueueTopicId, err)
						forwarderStats.AddLost(elem.CompanyID)
						continue
					} else {
						forwarderStats.AddEnterQueueAtH(elem.CompanyID)
					}
				}
			}

		} (i, forwardWaitGroup)
	}
}

func takeDownAsyncFanout(pubsubFailureChan *chan *forwarderPubsub.PubSubElement, waitGroup *sync.WaitGroup) {
	for i:=0; i<nbrPublishWorkers; i++ {
		*pubsubFailureChan <- nil
	}

	waitGroup.Wait()
}

func cleanup() {
	forwarderDb.Cleanup()
	//forwarderStats.CleanupV2()  // done in WriteStatsToDb()
}

func Fanout(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
	err := env()
	if nil != err {
		return fmt.Errorf("webhook responder is mis configured: %v", err)
	}

	defer cleanup()

	// Check if DB is happy. If it's not, then don't do anything this time and retry on next tick.
	err = forwarderDb.CheckDb()
	if nil != err {
		fmt.Printf("forwarder.fanout.Fanout(%s): Db check failed: %v\n", devprod, err)
		return err
	}

	// The webhook posts to the topic feeding inSubscriptionId

	// ReceiveEventsFromPubsub is a blocking function that populates pubsubForwardChan with at most maxNbrMessagesPolled
	// messages

	// All these messages are
	// a) verified
	// b) put one time on outQueueTopicId per forward target url. So if we have 2 forward endpoint, 2 messages are enqueued.

	pubsubForwardChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(pubsubForwardChan)
	var forwardWaitGroup sync.WaitGroup

	asyncFanout(&pubsubForwardChan, &forwardWaitGroup)

	// This one starts and takes down the ackQueue
	_, err = forwarderPubsub.ReceiveEventsFromPubsub(devprod, projectId, inSubscriptionId, minAge, maxNbrMessagesPolled, &pubsubForwardChan, maxPubsubQueueIdleMs, maxOutstandingMessages)
	if nil != err {
		// Super important too.
		fmt.Printf("forwarder.fanout.Fanout(%s) failed to receive events: %v\n", devprod, err)
	}

	takeDownAsyncFanout(&pubsubForwardChan, &forwardWaitGroup)

	nbrReceived, _, nbrLost, _ := forwarderDb.WriteStatsToDb()

	fmt.Printf("forwarder.fanout.Fanout(%s): done. v.%s # received: %d, # drop: %d,  Memstats: %s\n", devprod, forwarderCommon.PackageVersion, nbrReceived, nbrLost, forwarderStats.GetMemUsageStr())

	return nil
}
