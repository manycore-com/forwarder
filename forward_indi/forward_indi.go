package forward_indi

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderEsp "github.com/manycore-com/forwarder/esp"
	forwarderIQ "github.com/manycore-com/forwarder/individual_queues"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	forwarderTriggerIndi "github.com/manycore-com/forwarder/trigger_indi"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var projectId = ""
var hashId = 0
var inSubscriptionTemplate = ""
var minAgeSecs = -1 // No delay!
var devprod = ""    // Optional: We use dev for development, devprod for live test, prod for live
var nbrAckWorkers = 32
var nbrPublishWorkers = 32
var maxNbrMessagesPolled = 64
var atQueue = -1
var maxPubsubQueueIdleMs = 250
var maxMessageAge = 3600 * 72  // messages can now be 72h
var maxOutstandingMessages = 32
var secondsThresholdToNextQueue = -1
var nextQueueTopicId = ""
func env() error {
	projectId = os.Getenv("PROJECT_ID")
	inSubscriptionTemplate = os.Getenv("IN_SUBSCRIPTION_TEMPLATE")

	if projectId == "" {
		return fmt.Errorf("missing PROJECT_ID environment variable")
	}

	if inSubscriptionTemplate == "" {
		return fmt.Errorf("missing IN_SUBSCRIPTION_TEMPLATE environment variable")
	}

	devprod = os.Getenv("DEV_OR_PROD")

	var err error
	err = forwarderCommon.Env()
	if nil != err {
		return fmt.Errorf("forward_indi.env() Failed to initialize common: %v", err)
	}

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

	if "" != os.Getenv("SECONDS_THRESHOLD_TO_NEXT_QUEUE") {
		secondsThresholdToNextQueue, err = strconv.Atoi(os.Getenv("SECONDS_THRESHOLD_TO_NEXT_QUEUE"))
		if nil != err {
			return fmt.Errorf("failed to parse integer SECONDS_THRESHOLD_TO_NEXT_QUEUE: %v", err)
		}

		if 0 > secondsThresholdToNextQueue {
			return fmt.Errorf("optional SECONDS_THRESHOLD_TO_NEXT_QUEUE must be positive to make sense")
		}
	}

	if "" != os.Getenv("NEXT_QUEUE_TOPIC_ID") {
		nextQueueTopicId = os.Getenv("NEXT_QUEUE_TOPIC_ID")
	}

	return nil
}

var redisKeyToLowestEpoch = make(map[string]int64)
var redisKeyToNbrMsg = make(map[string]int)
var resendDataMutex sync.Mutex

func updateResendData(redisAgeKey string, redisCountKey string, ts int64) {
	resendDataMutex.Lock()
	defer resendDataMutex.Unlock()

	redisKeyToNbrMsg[redisCountKey] += 1

	if int64(0) == redisKeyToLowestEpoch[redisAgeKey] || ts < redisKeyToLowestEpoch[redisAgeKey] {
		redisKeyToLowestEpoch[redisAgeKey] = ts
	}
}

func asyncFailureProcessing(
	pubsubFailureChan *chan *forwarderPubsub.PubSubElement,
	failureWaitGroup *sync.WaitGroup,
	outTopicIds [3]string,
	outThresholds [3]int64) {

	var redisKeysAgeNoEndpointId = [3]string{
		"oldest_" + outTopicIds[0] + "_",
		"oldest_" + outTopicIds[1] + "_",
		"oldest_" + outTopicIds[2] + "_",
	}

	var redisKeysCountingNoEndpointId = [3]string{
		"counting_" + outTopicIds[0] + "_",
		"counting_" + outTopicIds[1] + "_",
		"counting_" + outTopicIds[2] + "_",
	}

	for i:=0; i<nbrPublishWorkers; i++ {
		failureWaitGroup.Add(1)
		go func(idx int, failureWaitGroup *sync.WaitGroup) {
			defer failureWaitGroup.Done()

			ctx1, client, err := forwarderPubsub.SetupClient(projectId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				// TODO: propagate error
				fmt.Printf("forwarder.forward_indi.asyncFailureProcessing(%s,%d): Critical Error: Failed to instantiate Client: %v\n", devprod, idx, err)
				return
			}

			topicIdToTopic := map[string]*pubsub.Topic{}
			for {
				elem := <- *pubsubFailureChan

				if nil == elem {
					//fmt.Printf("forwarder.forward_indi.asyncFailureProcessing(%s,%d): done.\n", devprod, idx)
					break
				}

				var messageAge = time.Now().Unix() - elem.Ts
				var outQueueTopicId = ""
				var redisAgeKey = ""
				var redisCountKey = ""
				var found = false
				for offs, topicThreshold := range outThresholds {
					if messageAge >= topicThreshold {
						found = true
						outQueueTopicId = outTopicIds[offs]
						redisAgeKey = redisKeysAgeNoEndpointId[offs] + strconv.Itoa(elem.EndPointId)
						redisCountKey = redisKeysCountingNoEndpointId[offs] + strconv.Itoa(elem.EndPointId)
					}
				}

				if ! found {
					fmt.Printf("forwarder.forward_indi.asyncFailureProcessing() wrong age:%d, moving to first queue", messageAge)
					outQueueTopicId = outTopicIds[2]
				}

				if _, ok := topicIdToTopic[outQueueTopicId]; ! ok {
					topicIdToTopic[outQueueTopicId] = client.Topic(outQueueTopicId)
				}
				var outTopic = topicIdToTopic[outQueueTopicId]

				err = forwarderPubsub.PushAndWaitElemToPubsub(ctx1, outTopic, elem)
				if err != nil {
					forwarderStats.AddLost(elem.CompanyID, elem.EndPointId)
					fmt.Printf("forwarder.forward_indi.asyncFailureProcessing(%s,%d): Error: Failed to send to %s pubsub: %v\n", devprod, idx, outQueueTopicId, err)
					continue
				}

				updateResendData(redisAgeKey, redisCountKey, elem.Ts)

				fmt.Printf("forwarder.forward_indi.asyncFailureProcessing(%s,%d): Success. Forwarded topic=%s package=%#v\n", devprod, idx, outQueueTopicId, elem)
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

var forwardIndiNbrForwarded = 0
var forwardIndiNbrDropped = 0
var forwardIndiNbrFail = 0
var forwardIndiMutex sync.Mutex

func incNbrForwarded() {
	forwardIndiMutex.Lock()
	forwardIndiNbrForwarded++
	forwardIndiMutex.Unlock()
}

func incNbrDropped() {
	forwardIndiMutex.Lock()
	forwardIndiNbrDropped++
	forwardIndiMutex.Unlock()
}

func incNbrFail() {
	forwardIndiMutex.Lock()
	forwardIndiNbrFail++
	forwardIndiMutex.Unlock()
}

func sendWarning(elem *forwarderPubsub.PubSubElement, hours int) {
	ud, err := forwarderDb.GetUserData(elem.CompanyID)
	if nil != err {
		fmt.Printf("forward_indi.sendWarning() Failed to get user data: %v\n", err)
	} else {

		cfg, err := forwarderIQ.GetEndPointData(elem.EndPointId)
		if err != nil {
			fmt.Printf("forward_indi.sendWarning() Failed to get user data: %v\n", err)
			return
		}

		subject := fmt.Sprintf("InboxBooster: Warning: failed to forward %d hours", hours)

		msg := fmt.Sprintf("Hi\n\nYour have messages that failed to forward for %d hours.\n\n" +
			"Endpoint:\n%s\n\nWe'll keep retrying for 64 hours\n\nRegards,\nInboxbooster staff.\n",
			hours, cfg.ForwardEndpoint)

		err = forwarderCommon.SendMail(
			forwarderCommon.GetDefaultFrom(os.Getenv("FORWARDER_EMAIL_REPLY_TO")),
			os.Getenv("FORWARDER_EMAIL_REPLY_TO"),
			[]string{ud.AlertEmail},
			nil,
			[]string{"alerts@manycore.io"},
			subject,
			msg)
	}
}

func warnIfOld(oldElements map[int]*forwarderPubsub.PubSubElement) int {
	mailSent := 0

	var twoDaysIfTsLt = time.Now().Unix() - 48 * 3600

	for _, elem := range oldElements {
		key := fmt.Sprintf("WARNED_24H_OR_48h_%d", elem.EndPointId)

		var age = 24
		if elem.Ts < twoDaysIfTsLt {
			age = 48
		}

		val, err := forwarderRedis.GetInt(key)
		if nil != err {
			fmt.Printf("forward_indi.warIfOld() Error: Failed to read value in redis for key=%s: %v\n", key, err)
			return mailSent // redis is down
		}

		if age > val {

			err = forwarderRedis.SetInt64(key, int64(age))
			if nil != err {
				fmt.Printf("forward_indi.warIfOld() Error: Failed to set value in redis for key=%s: %v\n", key, err)
				return mailSent
			}

			_, err = forwarderRedis.Expire(key, 3600)
			if nil != err {
				fmt.Printf("forward_indi.warIfOld() Failed to set ttl in redis for key=%s: %v\n", key, err)
				// Better to now have any than to blindly
				forwarderRedis.Del(key)
				return mailSent
			}

			sendWarning(elem, age)
			mailSent++
		} else {
			fmt.Printf("forward_indi.warIfOld() %dh old messages already warned about for endpoint=%d\n", age, elem.EndPointId)
		}
	}

	return mailSent
}

func asyncForward(pubsubForwardChan *chan *forwarderPubsub.PubSubElement, forwardWaitGroup *sync.WaitGroup, pubsubFailureChan *chan *forwarderPubsub.PubSubElement) {

	var now = time.Now().Unix()
	var dieIfTsLt = now - int64(maxMessageAge)

	var oneDayIfTsLt = now - 24 * 3600
	var twoDaysIfTsLt = now - 48 * 3600
	var oldElements = map[int]*forwarderPubsub.PubSubElement{}

	for i := 0; i < nbrPublishWorkers; i++ {
		forwardWaitGroup.Add(1)

		go func(idx int, forwardWaitGroup *sync.WaitGroup) {
			defer forwardWaitGroup.Done()

			for {
				elem := <- *pubsubForwardChan
				if nil == elem {
					//fmt.Printf("forwarder.forward_indi.asyncForward(%s,%d): done.\n", devprod, idx)
					break
				}

				if elem.Ts < dieIfTsLt {
					fmt.Printf("forwarder.forward_indi.asyncForward(%s) Package died of old age\n", devprod)
					forwarderStats.AddTimeout(elem.CompanyID, elem.EndPointId)
					incNbrDropped()
					continue
				}

				if elem.Ts < twoDaysIfTsLt {
					oldElements[elem.EndPointId] = elem
				} else if elem.Ts < oneDayIfTsLt {
					if nil == oldElements[elem.EndPointId] {
						oldElements[elem.EndPointId] = elem
					}
				}

				var err error = nil
				var anyPointToRetry bool
				//
				// This is where you add new ESP.
				//
				if elem.ESP == "sg" {
					err, anyPointToRetry = forwarderEsp.ForwardSg(devprod, elem)
				} else {
					fmt.Printf("forwarder.forward_indi.asyncForward(%s): Bad esp. esp=%s, companyId=%d\n", devprod, elem.ESP, elem.CompanyID)
					forwarderStats.AddLost(elem.CompanyID, elem.EndPointId)
					forwarderStats.AddErrorMessage(elem.CompanyID, elem.EndPointId, "Only sendgrid is supported for now. esp=" + elem.ESP)
					incNbrDropped()
					continue
				}

				if nil == err {
					forwarderStats.AddForwardedAtH(elem.CompanyID, elem.EndPointId)
					forwarderStats.AddAgeWhenForward(elem.CompanyID, elem.EndPointId, elem.Ts)
					incNbrForwarded()
				} else {

					fmt.Printf("forwarder.forward_indi.asyncForward(%s): Failed to forward: %v, retryable error:%v\n", devprod, err, anyPointToRetry)

					if anyPointToRetry {
						// Stats calculated by asyncFailureProcessing
						incNbrFail()
						*pubsubFailureChan <- elem
					} else {
						forwarderStats.AddLost(elem.CompanyID, elem.EndPointId)
						incNbrDropped()
					}
				}
			}

		} (i, forwardWaitGroup)
	}

	warnIfOld(oldElements)
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

	forwardIndiNbrForwarded = 0
	forwardIndiNbrDropped = 0
	forwardIndiNbrFail = 0

	redisKeyToLowestEpoch = make(map[string]int64)
	redisKeyToNbrMsg = make(map[string]int)
}

func ForwardIndi(ctx context.Context, m forwarderPubsub.PubSubMessage, outTopicIds [3]string, outThresholds [3]int64, triggerMyselfTopicId string) error {
	err := env()
	if nil != err {
		return fmt.Errorf("forwarder.forward_indi.ForwardIndi(): v%s webhook responder is mis configured: %v", forwarderCommon.PackageVersion, err)
	}

	defer cleanup()

	// Check if DB is happy. If it's not, then don't do anything this time and retry on next tick.
	err = forwarderDb.CheckDb()
	if nil != err {
		fmt.Printf("forwarder.forward_indi.ForwardIndi(): v%s Db check failed: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	if forwarderDb.IsPaused(hashId) {
		fmt.Printf("forwarder.forward_indi.ForwardIndi() v%s We're in PAUSE\n", forwarderCommon.PackageVersion)
		return nil
	}

	err = forwarderRedis.Init()
	if nil != err {
		return fmt.Errorf("forwarder.forward_indi.ForwardIndi() v%s failed to init redis: %v", forwarderCommon.PackageVersion, err)
	}
	defer forwarderRedis.Cleanup()

	defer forwarderIQ.Cleanup()

	var trgmsg forwarderTriggerIndi.TriggerIndiElement
	err = json.Unmarshal(m.Data, &trgmsg)
	if nil != err {
		return fmt.Errorf("forwarder.forward_indi.ForwardIndi() v%s Error decoding trigger message: %v", forwarderCommon.PackageVersion, err)
	} else {
		fmt.Printf("forwarder.forward_indi.ForwardIndi() v%s got trigger message: %#v\n", forwarderCommon.PackageVersion, trgmsg)
	}

	pubsubFailureChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(pubsubFailureChan)
	var failureWaitGroup sync.WaitGroup

	pubsubForwardChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(pubsubForwardChan)
	var forwardWaitGroup sync.WaitGroup

	asyncFailureProcessing(&pubsubFailureChan, &failureWaitGroup, outTopicIds, outThresholds)

	asyncForward(&pubsubForwardChan, &forwardWaitGroup, &pubsubFailureChan)

	// This one starts and takes down the ackQueue
	var inSubscriptionId = fmt.Sprintf(inSubscriptionTemplate, trgmsg.EndPointId)
	_, err = forwarderPubsub.ReceiveEventsFromPubsub(projectId, inSubscriptionId, minAgeSecs, trgmsg.NbrItems, &pubsubForwardChan, maxPubsubQueueIdleMs, maxOutstandingMessages)
	if nil != err {
		// Super important too.
		fmt.Printf("forwarder.forward_indi.ForwardIndi(): v%s failed to receive events: %v\n", forwarderCommon.PackageVersion, err)
	}

	takeDownAsyncForward(&pubsubForwardChan, &forwardWaitGroup)

	takeDownAsyncFailureProcessing(&pubsubFailureChan, &failureWaitGroup)

	nbrOnPS, err := forwarderRedis.IncrBy("FWD_IQ_PS_" + strconv.Itoa(trgmsg.EndPointId), 0 - trgmsg.NbrItems)
	if nil != err {
		fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to decrease FWD_IQ_PS_%d by %d: %v\n", trgmsg.EndPointId, trgmsg.NbrItems, err)
	} else {
		fmt.Printf("forwarder.forward_indi.ForwardIndi() decreased FWD_IQ_PS_%d by %d to %d\n", trgmsg.EndPointId, trgmsg.NbrItems, nbrOnPS)
	}

	var nbrProcessed = forwardIndiNbrFail + forwardIndiNbrForwarded + forwardIndiNbrDropped

	var nbrOnQS = 0
	var hasNbrOnQS = false
	if 0 != (trgmsg.NbrItems - nbrProcessed) {
		var increaseQsBy = (trgmsg.NbrItems - nbrProcessed)
		nbrOnQS, err = forwarderRedis.IncrBy("FWD_IQ_QS_" + strconv.Itoa(trgmsg.EndPointId), increaseQsBy)
		if nil != err {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to increase FWD_IQ_QS_%d by %d: %v\n", trgmsg.EndPointId, increaseQsBy, err)
		} else {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() increased FWD_IQ_QS_%d by %d to %d because they were missed\n", trgmsg.EndPointId, increaseQsBy, nbrOnQS)
			hasNbrOnQS = true
		}
	}

	// Currently nothing is being processed. This means we'll have to wait for the trigger to kick in.
	if 0 == nbrOnPS {
		rand.Seed(time.Now().UnixNano())

		if ! hasNbrOnQS {
			nbrOnQS, err = forwarderRedis.GetInt("FWD_IQ_QS_" + strconv.Itoa(trgmsg.EndPointId))
			if nil != err {
				fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to read FWD_IQ_QS_%d: %v\n", trgmsg.EndPointId, err)
			} else {
				hasNbrOnQS = true
			}
		}

		// Smth must have been processed this time to self trigger. Redis numbers may be off etc.
		// We must have been able to read QS
		// There must be at least 8 items on QS, else price runs amok
		// 1 in 10 it stops because of the random number
		// Only self trigger if we're not around the time where we paranoia checks queue size
		var now = time.Now().UTC()
		if 0 < nbrProcessed && hasNbrOnQS && nbrOnQS > 8 && rand.Intn(10) < 9 && now.Minute() > 5 {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() decided to self trigger. PS=0, QS=%d\n", nbrOnQS)

			if nbrOnQS > 16 {
				nbrOnQS = 16
			}

			ctx, client, outQueueTopic, err := forwarderPubsub.SetupClientAndTopic(projectId, triggerMyselfTopicId)
			if nil != client {
				defer client.Close()
			}

			if nil != err {
				fmt.Printf("forwarder.forward_indi.ForwardIndi() failed create client for Trigger Myself: %v\n", err)
			} else {
				var jsonmsg = fmt.Sprintf("{\"EndPointId\":%d, \"NbrItems\":%d}", trgmsg.EndPointId, nbrOnQS)
				err = forwarderPubsub.PushAndWaitJsonStringToPubsub(ctx, outQueueTopic, jsonmsg)
				if nil != err {
					fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to post self trigger msg: %v\n", err)
				} else {
					val, err := forwarderRedis.IncrBy("FWD_IQ_PS_" + strconv.Itoa(trgmsg.EndPointId), nbrOnQS)
					if nil != err {
						fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to increase FWD_IQ_PS_%d by %d (self trigger): %v\n", trgmsg.EndPointId, nbrOnQS, err)
					} else {
						fmt.Printf("forwarder.forward_indi.ForwardIndi() increased FWD_IQ_PS_%d by %d to %d (self trigger)\n", trgmsg.EndPointId, nbrOnQS, val)
					}

					val, err = forwarderRedis.IncrBy("FWD_IQ_QS_" + strconv.Itoa(trgmsg.EndPointId), 0 - nbrOnQS)
					if nil != err {
						fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to decrease FWD_IQ_QS_%d by %d (self trigger): %v\n", trgmsg.EndPointId, nbrOnQS, err)
					} else {
						fmt.Printf("forwarder.forward_indi.ForwardIndi() decreased FWD_IQ_QS_%d by %d to %d (self trigger)\n", trgmsg.EndPointId, nbrOnQS, val)
					}
				}
			}
		}
	}

	_, nbrForwarded, nbrLost, nbrTimeout := forwarderDb.WriteStatsToDb()
	
	// Write the redis stats
	for redisCountKey, val := range redisKeyToNbrMsg {
		newVal, err := forwarderRedis.IncrBy(redisCountKey, val)
		if nil != err {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to increase redis %s by %d: %v\n", redisCountKey, val, err)
		} else {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() increased redis %s by %d to %d\n", redisCountKey, val, newVal)
		}

		_, err = forwarderRedis.Expire(redisCountKey, 48 * 3600)
		if nil != err {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to set expire on redis %s: %v\n", redisCountKey, err)
		}
	}

	for redisAgeKey, val := range redisKeyToLowestEpoch {
		newVal, err := forwarderRedis.SetToLowestInt64Not0(redisAgeKey, val)
		if nil != err {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() failed update redis %s by age %d: %v\n", redisAgeKey, val, err)
		} else {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() updated redis %s using %d to %d\n", redisAgeKey, val, newVal)
		}

		forwarderRedis.Expire(redisAgeKey, 48 * 3600)
		if nil != err {
			fmt.Printf("forwarder.forward_indi.ForwardIndi() failed to set expire on redis %s: %v\n", redisAgeKey, err)
		}
	}

	fmt.Printf("forwarder.forward_indi.ForwardIndi(): done. v%s # forward: %d, # drop: %d, # timeout: %d, Memstats: %s\n", forwarderCommon.PackageVersion, nbrForwarded, nbrLost, nbrTimeout, forwarderStats.GetMemUsageStr())

	return nil
}


