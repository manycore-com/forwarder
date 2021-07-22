package fanout_indi

import (
	"cloud.google.com/go/pubsub"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
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
var destTopicTemplate = "" // "INBOXBOOSTER_DEVPROD_FORWARD_INDI_%d"
var minAge = -1            // No delay!
var devprod = ""           // Optional: We use dev for development, devprod for live test, prod for live
var nbrAckWorkers = 32
var nbrPublishWorkers = 32
var maxNbrMessagesPolled = 64
var maxPubsubQueueIdleMs = 250
var maxOutstandingMessages = 32
func env() error {
	projectId = os.Getenv("PROJECT_ID")
	inSubscriptionId = os.Getenv("IN_SUBSCRIPTION_ID")
	destTopicTemplate = os.Getenv("DEST_TOPIC_TEMPLATE")

	if projectId == "" {
		return fmt.Errorf("missing PROJECT_ID environment variable")
	}

	devprod = os.Getenv("DEV_OR_PROD")

	if "" == inSubscriptionId {
		return fmt.Errorf("mandatory IN_SUBSCRIPTION_ID environment variable missing")
	}

	if "" == destTopicTemplate {
		return fmt.Errorf("mandatory DEST_TOPIC_TEMPLATE environment variable missing")
	}

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

var usedEndPoints map[int]int
var usedEndPointsMutex sync.Mutex
func addEndPointId(endPointId int) {
	usedEndPointsMutex.Lock()
	defer usedEndPointsMutex.Unlock()

	if 0 == usedEndPoints[endPointId] {
		usedEndPoints[endPointId] = 1
	} else {
		usedEndPoints[endPointId] += 1
	}
}

func asyncFanout(pubsubForwardChan *chan *forwarderPubsub.PubSubElement, forwardWaitGroup *sync.WaitGroup) {
	usedEndPoints = make(map[int]int)

	for i := 0; i < nbrPublishWorkers; i++ {
		forwardWaitGroup.Add(1)

		go func(idx int, forwardWaitGroup *sync.WaitGroup) {
			defer forwardWaitGroup.Done()

			ctx, client, err := forwarderPubsub.SetupClient(projectId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				fmt.Printf("forwarder.fanout_indi.asyncFanout(%d): Critical Error: Failed to instantiate Topic Client: %v\n", idx, err)
				return
			}

			ignoreCompany := map[int]bool{}
			hasSetMessage := map[int]bool{}
			endPointIdToTopic := map[int]*pubsub.Topic{}
			for {
				elem := <- *pubsubForwardChan
				if nil == elem {
					//fmt.Printf("forwarder.fanout_indi.asyncFanout(%s,%d): done.\n", devprod, idx)
					break
				}

				// First, determine where to send it.
				if ignoreCompany[elem.CompanyID] {
					continue
				}

				ci, err := forwarderDb.GetUserData(elem.CompanyID)
				if nil != err {
					fmt.Printf("forwarder.fanout_indi.asyncFanout() failed to get user data: %v\n", err)
					ignoreCompany[elem.CompanyID] = true
					continue
				}

				if 0 == len(ci.EndPoints) {
					ignoreCompany[elem.CompanyID] = true
					continue
				}

				for _, endPoint := range ci.EndPoints {
					fmt.Printf("forwarder.fanout_indi.asyncFanout() endpoint:%d\n", endPoint.EndPointId)
					elem.EndPointId = endPoint.EndPointId

					addEndPointId(endPoint.EndPointId)

					hashHead := CalculateSafeHashFromSecret(ci.Secret)

					if hashHead != elem.SafeHash {
						fmt.Printf("forwarder.fanout_indi.asyncFanout(): Safe hash is wrong. companyId=%d wrongHash=%s\n", elem.CompanyID, elem.SafeHash)
						forwarderStats.AddLost(elem.CompanyID, elem.EndPointId)
						continue
					}

					forwarderStats.AddReceivedAtH(elem.CompanyID, elem.EndPointId)

					if ! hasSetMessage[elem.CompanyID] {
						hasSetMessage[elem.CompanyID] = true
						payload, err := json.Marshal(elem)
						if nil == err {
							forwarderStats.AddExample(elem.CompanyID, elem.EndPointId, string(payload))
						} else {
							fmt.Printf("forwarder.fanout_indi.asyncFanout(%d): failed to Marshal element. companyId=%d err=%v\n", idx, elem.CompanyID, err)
						}
					}

					var outQueueTopicId = fmt.Sprintf(destTopicTemplate, elem.EndPointId)
					if _, ok := endPointIdToTopic[elem.EndPointId]; ! ok {
						endPointIdToTopic[elem.EndPointId] = client.Topic(outQueueTopicId)
					}
					var outTopic = endPointIdToTopic[elem.EndPointId]

					err = forwarderPubsub.PushAndWaitElemToPubsub(ctx, outTopic, elem)
					if err != nil {
						fmt.Printf("forwarder.fanout_indi.asyncFanout(%d): Error: Failed to send to %s pubsub: %v\n", idx, outQueueTopicId, err)
						forwarderStats.AddLost(elem.CompanyID, elem.EndPointId)
						continue
					} else {
						forwarderStats.AddEnterQueueAtH(elem.CompanyID, elem.EndPointId)
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

func FanoutIndi(ctx context.Context, m forwarderPubsub.PubSubMessage, hashId int) error {
	err := env()
	if nil != err {
		return fmt.Errorf("forwarder.fanout_indi.FanoutIndi(): webhook responder is mis configured: %v", err)
	}

	defer cleanup()

	// Check if DB is happy. If it's not, then don't do anything this time and retry on next tick.
	err = forwarderDb.CheckDb()
	if nil != err {
		fmt.Printf("forwarder.fanout_indi.FanoutIndi(): Db check failed: %v\n", err)
		return err
	}

	defer forwarderRedis.Cleanup()

	if forwarderDb.IsPaused(hashId) {
		fmt.Printf("forwarder.fanout_indi.FanoutIndi() We're in PAUSE\n")
		return nil
	}

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.fanout_indi.FanoutIndi(): Failed to init Redis: %v\n", err)
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
		fmt.Printf("forwarder.fanout_indi.FanoutIndi() failed to receive events: %v\n", err)
	}

	takeDownAsyncFanout(&pubsubForwardChan, &forwardWaitGroup)

	// Store endpoint keys to active set.
	for endPointId, count := range usedEndPoints {
		_, err = forwarderRedis.SetAddMember("FWD_IQ_ACTIVE_ENDPOINTS_SET", endPointId)
		if err != nil {
			// No point in returning an error here. 
			fmt.Printf("forwarder.fanout_indi.FanoutIndi() failed to add active endpoint: %v\n", err)
		}

		// Increase FWD_IQ_QS_#
		_, err = forwarderRedis.IncrBy("FWD_IQ_QS_" + strconv.Itoa(endPointId), count)
		if nil != err {
			fmt.Printf("forwarder.fanout_indi.FanoutIndi() failed to increase queues size in redis: %v\n", err)
		}
	}

	nbrReceived, _, nbrLost, _ := forwarderDb.WriteStatsToDb()

	fmt.Printf("forwarder.fanout_indi.FanoutIndi(): done. v%s # received: %d, # drop: %d,  Memstats: %s\n", forwarderCommon.PackageVersion, nbrReceived, nbrLost, forwarderStats.GetMemUsageStr())

	return err
}
