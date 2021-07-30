package individual_queues

import (
	"cloud.google.com/go/pubsub"
	"context"
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
	"time"
)

var endpointIdToCfg = make(map[int]*forwarderDb.EndPointCfgStruct)

var touchedForwardId = make(map[int]bool)

var iqMutex sync.Mutex

var projectId = ""
var hashId = 0
var nbrPublishWorkers = 32
var maxNbrMessagesPolled = 64
var maxPubsubQueueIdleMs = 1200
var maxOutstandingMessages = 32
func Env() error {
	var err error
	projectId = os.Getenv("PROJECT_ID")

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

func Cleanup() {
	endpointIdToCfg = make(map[int]*forwarderDb.EndPointCfgStruct)
	touchedForwardId = make(map[int]bool)
}

// TouchForwardId prepare a forwardId for use
func TouchForwardId(forwardId int) error {
	if touchedForwardId[forwardId] {
		return nil
	}

	_, err := forwarderRedis.SetAddMember("FWD_IQ_ACTIVE_ENDPOINTS_SET", forwardId)
	if nil != err {
		fmt.Printf("forwarder.IQ.TouchForwardId() error: %v\n", err)
		return err
	}

	return nil
}

// GetEndPointData gives us the forward URL and some other data for the particular endpoint.
// This method caches in ram + redis from db. It's meant to be able to be called from a loop without anyone getting upset.
func GetEndPointData(endPointId int) (*forwarderDb.EndPointCfgStruct, error) {
	iqMutex.Lock()
	defer iqMutex.Unlock()

	if val, ok := endpointIdToCfg[endPointId]; ok {
		return val, nil
	}

	fmt.Printf("forwarder.IQ.GetEndPointData(): Local cache miss, trying redis. id=%d\n", endPointId)

	// ok try Redis
	key := "FWD_IQ_GETENDPOINTDATA_" + strconv.Itoa(endPointId)
	byteArray, _ := forwarderRedis.Get(key)
	if nil != byteArray {
		var cfg forwarderDb.EndPointCfgStruct
		if err := json.Unmarshal(byteArray, &cfg); err != nil {
			return nil, err
		}
		endpointIdToCfg[endPointId] = &cfg
		return &cfg, nil
	}

	fmt.Printf("forwarder.IQ.GetEndPointData(): Redis miss, trying db. id=%d\n", endPointId)

	// wasn't in redis. Load from db.
	cfg, err := forwarderDb.GetEndPointCfg(endPointId)
	if nil != err {
		endpointIdToCfg[endPointId] = nil
		return nil, err
	}

	if cfg == nil {
		endpointIdToCfg[endPointId] = nil
		return nil, fmt.Errorf("forwarder.individual_queues.GetEndPointData() db returned nil cfg: %v", err)
	}

	endpointIdToCfg[endPointId] = cfg

	b, err := json.Marshal(cfg)
	if nil != err {
		return cfg, err
	}

	err = forwarderRedis.Set(key, b)
	if err != nil {
		return cfg, fmt.Errorf("forwarder.individual_queues.GetEndPointData() redis failed to cache cfg: %v", err)
	}

	_, err = forwarderRedis.Expire(key, 10 * 60)
	if nil != err {
		fmt.Printf("forwarder.IQ.GetEndPointData(): Failed to set TTL for %s: %v", key, err)
	}

	fmt.Printf("forwarder.IQ.GetEndPointData(): Success reading from db: %#v db:%s\n", cfg, os.Getenv("DB_NAME"))

	return cfg, nil
}

var endPointIdToCount = make(map[int]int)
var countMutex sync.Mutex

func incEndPointIdToCount(endPointId int) {
	countMutex.Lock()
	defer countMutex.Unlock()
	endPointIdToCount[endPointId] += 1
}

func asyncWriterToIndividualQueues(writerChan *chan *forwarderPubsub.PubSubElement,
	                               writerWaitGroup *sync.WaitGroup,
	                               destSubscriptionTemplate string) {

	endPointIdToCount = make(map[int]int)

	for i := 0; i < nbrPublishWorkers; i++ {
		writerWaitGroup.Add(1)

		go func(idx int, forwardWaitGroup *sync.WaitGroup) {
			defer forwardWaitGroup.Done()

			ctx := context.Background()
			client, err := pubsub.NewClient(ctx, projectId)
			if err != nil {
				fmt.Printf("forwarder.IQ.asyncWriterToIndividualQueues() Error: Failed to create client: %v\n", err)
				return
			}

			if nil != client {
				defer client.Close()
			}

			var endpointToTopicMap = make(map[int]*pubsub.Topic)

			for {
				elem := <- *writerChan
				if nil == elem {
					break
				}

				if 0 == elem.EndPointId {
					fmt.Printf("forwarder.IQ.asyncWriterToIndividualQueues() Error: endpoint id is 0\n")
					continue
				}

				topicId := fmt.Sprintf(destSubscriptionTemplate, elem.EndPointId)
				if _, ok := endpointIdToCfg[elem.EndPointId]; ! ok {
					newObj := client.Topic(topicId)
					endpointToTopicMap[elem.EndPointId] = newObj
				}

				topic := endpointToTopicMap[elem.EndPointId]
				err = forwarderPubsub.PushAndWaitElemToPubsub(&ctx, topic, elem)
				if err != nil {
					fmt.Printf("forwarder.IQ.asyncWriterToIndividualQueues() Error: Failed to forward to individual endpoint %s: %v\n", topicId, err)
					// TODO push back to original queue? It's being consumed from.
					continue
				}

				incEndPointIdToCount(elem.EndPointId)

				fmt.Printf("forwarder.IQ.asyncWriterToIndividualQueues() Success forwarding to %s\n", topicId)
			}

		} (i, writerWaitGroup)
	}
}

// PullAllSubscriptions should also do stats for queue age etc.
func PullAllSubscriptions(sourceSubscriptionIds []string, writerChan *chan *forwarderPubsub.PubSubElement) bool {
	var waitGroup sync.WaitGroup
	var seriousError bool = false
	for _, sourceSubscriptionId := range sourceSubscriptionIds {
		waitGroup.Add(1)

		go func(waitGroup *sync.WaitGroup, sourceSubscription string, writerChan *chan *forwarderPubsub.PubSubElement) {
			defer waitGroup.Done()

			ctx := context.Background()
			client, clientErr := pubsub.NewClient(ctx, projectId)
			if clientErr != nil {
				fmt.Printf("forwarder.IQ.PullAllSubscriptions(%s) failed to create client: %v\n", sourceSubscription, clientErr)
				seriousError = true
				return
			}

			if nil != client {
				defer client.Close()
			}

			subscription := client.Subscription(sourceSubscription)
			subscription.ReceiveSettings.Synchronous = true
			subscription.ReceiveSettings.MaxOutstandingMessages = nbrPublishWorkers
			subscription.ReceiveSettings.MaxOutstandingBytes = 11000000
			fmt.Printf("Source subscription: %v\n", sourceSubscription)

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(500) * time.Second)
			defer cancel()

			var mu sync.Mutex
			var runTick = true
			var startMs = time.Now().UnixNano() / 1000000
			var lastAtMs = startMs + 5000 // 3000ms was too low for local machine. 8000ms was enough. It's reset after first received message.
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
					if (int64(6000) + copyOfLastAtMs) < rightNow {
						fmt.Printf("forwarder.IQ.PullAllSubscriptions.func(%s) Killing Receive due to %dms inactivity.\n", sourceSubscription, 6000)
						mu.Lock()
						runTick = false
						mu.Unlock()
						cancel()
						return
					}
				}
			} ()

			err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				mu.Lock()
				var runTickCopy = runTick
				mu.Unlock()

				if ! runTickCopy {
					fmt.Printf("forwarder.IQ.PullAllSubscriptions.Receive(%s) we are canceled.\n", sourceSubscription)
					defer msg.Nack()
					return
				}

				var elem forwarderPubsub.PubSubElement
				err := json.Unmarshal(msg.Data, &elem)

				if nil == err {
					mu.Lock()

					lastAtMs = time.Now().UnixNano() / 1000000

					mu.Unlock()

					fmt.Printf("forwarder.IQ.PullAllSubscriptions.Receive(%s): counted message %v\n", sourceSubscription, elem)
					*writerChan <- &elem
					msg.Ack()

				} else {
					fmt.Printf("forwarder.IQ.PullAllSubscriptions.Receive(%s): Error: failed to Unmarshal: %v\n", sourceSubscription, err)
					msg.Ack()  // Valid or not, Ack to get rid of it
				}
			})

			if nil != err {
				fmt.Printf("forwarder.IQ.PullAllSubscriptions(%s) Received failed: %v\n", sourceSubscription, err)
				// The defer anonymous function will write -1 instead
				seriousError = true
			}

		} (&waitGroup, sourceSubscriptionId, writerChan)
	}

	waitGroup.Wait()

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.IQ.PullAllSubscriptions() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return seriousError
}

func reCalculateUsersQueueSizes_(endPointIdToSubsId map[int]string) error {
	var subscriptionIds []string
	for _, subsId := range endPointIdToSubsId {
		subscriptionIds = append(subscriptionIds, subsId)
	}

	if 0 == len(subscriptionIds) {
		fmt.Printf("forwarder.individual_queues.reCalculateUsersQueueSizes_() Got no subscriptionIds to check. Which can happen\n")
		return nil
	}

	subsToCount, err := forwarderPubsub.CheckNbrItemsPubsubs(projectId, subscriptionIds)
	if nil != err {
		return fmt.Errorf("forwarder.individual_queues.reCalculateUsersQueueSizes_() Failed to check queue sizes: %v", err)
	}

	// This is a set with all the endpoint ids we currently want to look at.
	forwarderRedis.Del("FWD_IQ_ACTIVE_ENDPOINTS_SET")
	for endPointId, subsName := range endPointIdToSubsId {

		if val, ok := subsToCount[subsName]; ok {
			forwarderRedis.SetInt64("FWD_IQ_QS_" + strconv.Itoa(endPointId), val)
			forwarderRedis.SetAddMember("FWD_IQ_ACTIVE_ENDPOINTS_SET", endPointId)
			fmt.Printf("forwarder.individual_queues.reCalculateUsersQueueSizes_() endpoint:%d qs:%d\n", endPointId, val)
		} else {
			forwarderRedis.SetInt64("FWD_IQ_QS_" + strconv.Itoa(endPointId), int64(0))
			fmt.Printf("forwarder.individual_queues.reCalculateUsersQueueSizes_() endpoint:%d qs:%d\n", endPointId, 0)
		}

		// Assume nothing is currently processing. This method is only called after a certain time of Pause.
		forwarderRedis.SetInt64("FWD_IQ_PS_" + strconv.Itoa(endPointId), int64(0))
	}

	return nil
}

// ReCalculateUsersQueueSizes is (should be) called after a break. So we can assume we can set currently processing to 0
func ReCalculateUsersQueueSizes(ctx context.Context, m forwarderPubsub.PubSubMessage, subscriptionTemplate string) error {

	err := Env()
	if nil != err {
		fmt.Printf("forwarder.IQ.ReCalculateUsersQueueSizes(): v%s Failed to setup Env: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer Cleanup()

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.IQ.ReCalculateUsersQueueSizes(): v%s Failed to init Redis: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer forwarderRedis.Cleanup()

	endPointIds, err := forwarderRedis.SetMembersInt("FWD_IQ_ACTIVE_ENDPOINTS_SET")
	if nil != err {
		return fmt.Errorf("forwarder.IQ.ReCalculateUsersQueueSizes() v%s failed to read set FWD_IQ_ACTIVE_ENDPOINTS_SET from Redis", forwarderCommon.PackageVersion)
	}

	var endPointIdToSubsId = make(map[int]string)
	for _, endPointId := range endPointIds {
		subs := fmt.Sprintf(subscriptionTemplate, endPointId)
		endPointIdToSubsId[endPointId] = subs
		fmt.Printf("forwarder.IQ.ReCalculateUsersQueueSizes() will check %s\n", subs)
	}

	fmt.Printf("forwarder.IQ.ReCalculateUsersQueueSizes() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, forwarderStats.GetMemUsageStr())

	return reCalculateUsersQueueSizes_(endPointIdToSubsId)
}
