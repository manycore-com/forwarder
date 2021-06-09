package forward

import (
	"context"
	"fmt"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderEsp "github.com/manycore-com/forwarder/esp"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"os"
	"strconv"
	"sync"
	"time"
)

var projectId = ""
var inSubscriptionId = ""
var outQueueTopicId = ""
var minAgeSecs = -1 // No delay!
var devprod = ""    // Optional: We use dev for development, devprod for live test, prod for live
var nbrAckWorkers = 32
var nbrPublishWorkers = 32
var maxNbrMessagesPolled = 64
var atQueue = -1
var maxPubsubQueueIdleMs = 250
var maxMessageAge = 3600 * 12
var version = "1"
func env() error {
	projectId = os.Getenv("PROJECT_ID")
	inSubscriptionId = os.Getenv("IN_SUBSCRIPTION_ID")
	outQueueTopicId = os.Getenv("OUT_QUEUE_TOPIC_ID")

	if projectId == "" {
		return fmt.Errorf("missing PROJECT_ID environment variable")
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

	if "" == os.Getenv("AT_QUEUE") {
		return fmt.Errorf("missing AT_QUEUE (valid values: 1 to 3)")
	} else {
		atQueue, err = strconv.Atoi(os.Getenv("AT_QUEUE"))
		if nil != err {
			return fmt.Errorf("failed to parse integer AT_QUEUE: %v", err)
		}

		if 1 > atQueue || 3 < atQueue {
			return fmt.Errorf("optional AT_QUEUE environent variable must be 1 to 3: %v", atQueue)
		}
	}

	if "" != os.Getenv("MIN_AGE_SECS") {
		minAgeSecs, err = strconv.Atoi(os.Getenv("MIN_AGE_SECS"))
		if nil != err {
			return fmt.Errorf("failed to parse integer MIN_AGE_SECS: %v", err)
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

	return nil
}

func asyncFailureProcessing(pubsubFailureChan *chan *forwarderPubsub.PubSubElement, failureWaitGroup *sync.WaitGroup, nextTopicId string) {
	for i:=0; i<nbrPublishWorkers; i++ {
		failureWaitGroup.Add(1)
		go func(idx int, failureWaitGroup *sync.WaitGroup) {
			defer failureWaitGroup.Done()

			ctx1, client, nextForwardTopic, err := forwarderPubsub.SetupClientAndTopic(projectId, nextTopicId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				fmt.Printf("forwarder.forward.asyncFailureProcessing(%s,%d): Critical Error: Failed to instantiate Client: %v\n", devprod, idx, err)
				return
			}

			for {
				elem := <- *pubsubFailureChan
				if nil == elem {
					//fmt.Printf("forwarder.forward.asyncFailureProcessing(%s,%d): done.\n", devprod, idx)
					break
				}

				if "" == nextTopicId {
					fmt.Printf("forwarder.forward.asyncFailureProcessing(%s,%d): Failure: too many errors: %#v\n", devprod, idx, elem)
					continue
				}

				err = forwarderPubsub.PushElemToPubsub(ctx1, nextForwardTopic, elem)
				if err != nil {
					forwarderStats.AddLost(elem.CompanyID)
					fmt.Printf("forwarder.forward.asyncFailureProcessing(%s,%d): Error: Failed to send to %s pubsub: %v\n", devprod, idx, nextTopicId, err)
					continue
				}

				fmt.Printf("forwarder.forward.asyncFailureProcessing(%s,%d): Success. Forwarded topic=%s package=%#v\n", devprod, idx, nextTopicId, elem)
			}
		} (i, failureWaitGroup)
	}

}

func takeDownAsyncFailureProcessing(pubsubFailureChan *chan *forwarderPubsub.PubSubElement, failureWaitGroup *sync.WaitGroup) {
	for i:=0; i<nbrPublishWorkers; i++ {
		*pubsubFailureChan <- nil
	}

	failureWaitGroup.Wait()
}


func asyncForward(pubsubForwardChan *chan *forwarderPubsub.PubSubElement, forwardWaitGroup *sync.WaitGroup, pubsubFailureChan *chan *forwarderPubsub.PubSubElement) {

	var dieIfTsLt = time.Now().Unix() - int64(maxMessageAge)
	for i := 0; i < nbrPublishWorkers; i++ {
		forwardWaitGroup.Add(1)

		go func(idx int, forwardWaitGroup *sync.WaitGroup) {
			defer forwardWaitGroup.Done()

			for {
				elem := <- *pubsubForwardChan
				if nil == elem {
					//fmt.Printf("forwarder.forward.asyncForward(%s,%d): done.\n", devprod, idx)
					break
				}

				if elem.Ts < dieIfTsLt {
					fmt.Printf("forwarder.forward.asyncForward(%s) Package died of old age\n", devprod)
					forwarderStats.AddTimeout(elem.CompanyID)
					continue
				}

				var err error = nil
				var anyPointToRetry bool
				//
				// This is where you add new ESP.
				//
				if elem.ESP == "sg" {
					err, anyPointToRetry = forwarderEsp.ForwardSg(devprod, elem)
				} else {
					fmt.Printf("forwarder.forward.asyncForward(%s): Bad esp. esp=%s, companyId=%d\n", devprod, elem.ESP, elem.CompanyID)
					forwarderStats.AddLost(elem.CompanyID)
					forwarderStats.AddErrorMessage(elem.CompanyID, "Only sendgrid is supported for now. esp=" + elem.ESP)
					continue
				}

				if nil == err {
					forwarderStats.AddForwardedAtH(elem.CompanyID)
					forwarderStats.AddAgeWhenForward(elem.CompanyID, elem.Ts)
				} else {

					fmt.Printf("forwarder.forward.asyncForward(%s): Failed to forward: %v, retryable error:%v\n", devprod, err, anyPointToRetry)

					if anyPointToRetry {
						// Stats calculated by asyncFailureProcessing
						*pubsubFailureChan <- elem
					} else {
						forwarderStats.AddLost(elem.CompanyID)
					}
				}
			}

		} (i, forwardWaitGroup)
	}
}

func takeDownAsyncForward(pubsubFailureChan *chan *forwarderPubsub.PubSubElement, forwardWaitGroup *sync.WaitGroup) {
	for i:=0; i<nbrPublishWorkers; i++ {
		*pubsubFailureChan <- nil
	}

	forwardWaitGroup.Wait()
}

func cleanup() {
	forwarderDb.Cleanup()
	//forwarderStats.CleanupV2()  // done in WriteStatsToDb()
}

func Forward(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
	err := env()
	if nil != err {
		return fmt.Errorf("forwarder.webhook.Forward() webhook responder is mis configured: %v", err)
	}

	defer cleanup()

	// Check if DB is happy. If it's not, then don't do anything this time and retry on next tick.
	err = forwarderDb.CheckDb()
	if nil != err {
		fmt.Printf("forwarder.forward.Forward(%s): Db check failed: %v\n", devprod, err)
		return err
	}

	pubsubFailureChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(pubsubFailureChan)
	var failureWaitGroup sync.WaitGroup

	pubsubForwardChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(pubsubForwardChan)
	var forwardWaitGroup sync.WaitGroup

	asyncFailureProcessing(&pubsubFailureChan, &failureWaitGroup, outQueueTopicId)

	asyncForward(&pubsubForwardChan, &forwardWaitGroup, &pubsubFailureChan)

	// This one starts and takes down the ackQueue
	_, err = forwarderPubsub.ReceiveEventsFromPubsub(devprod, projectId, inSubscriptionId, minAgeSecs, nbrAckWorkers, maxNbrMessagesPolled, &pubsubForwardChan, maxPubsubQueueIdleMs)
	if nil != err {
		// Super important too.
		fmt.Printf("forwarder.forward.Forward(%s) failed to receive events: %v\n", devprod, err)
	}

	takeDownAsyncForward(&pubsubForwardChan, &forwardWaitGroup)

	takeDownAsyncFailureProcessing(&pubsubFailureChan, &failureWaitGroup)

	_, nbrForwarded, nbrLost, nbrTimeout := forwarderDb.WriteStatsToDb()

	fmt.Printf("forwarder.forward.Forward(%s): done. v.%s # forward: %d, # drop: %d, # timeout: %d, Memstats: %s\n", devprod, version, nbrForwarded, nbrLost, nbrTimeout, forwarderStats.GetMemUsageStr())

	return nil
}


