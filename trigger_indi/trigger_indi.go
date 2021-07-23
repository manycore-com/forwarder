package trigger_indi

import (
	"context"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderIQ "github.com/manycore-com/forwarder/individual_queues"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"os"
	"strconv"
	"sync"
)

var maxNbrMessagesPolled = 64 // This should be the same in forwarder!
var subscriptionTemplate = "" // "INBOXBOOSTER_DEVPROD_FORWARD_INDI_%d"
var triggerTopicId = ""
var projectId = ""
var devprod = ""
var nbrPublishWorkers = 32
var triggerPackagesScaler float64 = 1.0  // Somehow it seems like we don't consume everything, so let's try surplus triggers
func env() error {
	var err error

	projectId = os.Getenv("PROJECT_ID")
	devprod = os.Getenv("DEV_OR_PROD")
	subscriptionTemplate = os.Getenv("SUBSCRIPTION_TEMPLATE")
	triggerTopicId = os.Getenv("TRIGGER_TOPIC_ID")

	if "" == subscriptionTemplate {
		return fmt.Errorf("mandatory SUBSCRIPTION_TEMPLATE environment variable missing")
	}

	if "" == triggerTopicId {
		return fmt.Errorf("mandatory TRIGGER_TOPIC_ID environment variable missing")
	}

	// This name is not great, but to keep it consistent with the naming in Forwarder
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

	if "" != os.Getenv("TRIGGER_PACKAGES_SCALAR") {
		triggerPackagesScaler, err = strconv.ParseFloat(os.Getenv("TRIGGER_PACKAGES_SCALAR"), 64)
		if err != nil {
			return fmt.Errorf("failed to parse optional float TRIGGER_PACKAGES_SCALAR: %v", err)
		}

		if 0.25 > triggerPackagesScaler {
			return fmt.Errorf("optional TRIGGER_PACKAGES_SCALAR environent variable must be at least 0.25: %f", triggerPackagesScaler)
		}

		if 5 < triggerPackagesScaler {
			return fmt.Errorf("optional TRIGGER_PACKAGES_SCALAR environent variable must be at max 5: %f", triggerPackagesScaler)
		}
	}

	return nil
}

type TriggerIndiElement struct {
	EndPointId    int
	NbrItems      int
}

func asyncSendTriggerPackages(channel *chan *TriggerIndiElement, waitGroup *sync.WaitGroup, nbrWorkers int) {
	for i := 0; i < nbrWorkers; i++ {
		waitGroup.Add(1)

		go func(idx int, waitGroup *sync.WaitGroup) {
			defer waitGroup.Done()

			ctx, client, err := forwarderPubsub.SetupClient(projectId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				fmt.Printf("forwarder.trigger_indi.asyncFanout(%d): Critical Error: Failed to instantiate Topic Client: %v\n", idx, err)
				return
			}

			outTopic := client.Topic(triggerTopicId)

			for {
				var msg = <- *channel
				if nil == msg {
					//fmt.Printf("forwarder.trigger.asyncSendTriggerPackages(%s,%d): trigger sender worker done\n", devprod, idx)
					return
				}

				var trgmsg = fmt.Sprintf("{\"EndPointId\":%d, \"NbrItems\":%d}", msg.EndPointId, msg.NbrItems)

				err := forwarderPubsub.PushAndWaitJsonStringToPubsub(ctx, outTopic, trgmsg)
				if err != nil {
					fmt.Printf("forwarder.trigger_indi.asyncFanout(%d): Failed to publish to trigger topic %s: %v\n", idx, triggerTopicId, err)
				}
			}

		}(i, waitGroup)
	}
}

func cleanup() {
	forwarderStats.CleanupV2()
}

func TriggerIndi(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
	defer cleanup()

	err := env()

	if nil != err {
		return fmt.Errorf("forwarder.trigger.TriggerIndi() v%s is mis configured: %v", forwarderCommon.PackageVersion, err)
	}

	fmt.Printf("forwarder.trigger.TriggerIndi(%s) Entry: Memstats: %s\n", devprod, forwarderStats.GetMemUsageStr())
	fmt.Printf("forwarder.trigger.TriggerIndi(%s) Checking size of trigger queue: proj:%s\n", devprod, projectId)

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.trigger_indi.TriggerIndi(): v%s Failed to init Redis: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	// Note that the active endpoint set is re-created hourly, and updated by forwarder_indi
	endPointIds, err := forwarderRedis.SetMembersInt("FWD_IQ_ACTIVE_ENDPOINTS_SET")
	if nil != err {
		return fmt.Errorf("forwarder.trigger_indi.TriggerIndi() v%s failed to read set FWD_IQ_ACTIVE_ENDPOINTS_SET from Redis", forwarderCommon.PackageVersion)
	}

	messageQueue := make(chan *TriggerIndiElement, nbrPublishWorkers)
	defer close(messageQueue)
	var waitGroup sync.WaitGroup

	asyncSendTriggerPackages(&messageQueue, &waitGroup, nbrPublishWorkers)

	for _, endPointId := range endPointIds {
		cfg, err := forwarderIQ.GetEndPointData(endPointId)
		if err != nil {
			fmt.Printf("forwarder.trigger_indi.TriggerIndi() Failed to get data for endPoint %d: %v\n", endPointId, err)
			continue
		}

		currentQueueSize, err := forwarderRedis.GetInt("FWD_IQ_QS_" + strconv.Itoa(endPointId))
		if nil != err {
			fmt.Printf("forwarder.trigger_indi.TriggerIndi() Failed to get redis FWD_IQ_QS_# size. endPoint %d: %v\n", endPointId, err)
			continue
		}

		currentPricessingSize, err := forwarderRedis.GetInt("FWD_IQ_PS_" + strconv.Itoa(endPointId))
		if nil != err {
			fmt.Printf("forwarder.trigger_indi.TriggerIndi() Failed to get redis FWD_IQ_PS_# processing. endPoint %d: %v\n", endPointId, err)
			continue
		}

		fmt.Printf("forwarder.trigger_indi.TriggerIndi() endpoint:%d qs:%d ps:%d maxConcurrent:%d\n", endPointId, currentQueueSize, currentPricessingSize, cfg.MaxConcurrentFwd)

		if currentPricessingSize < cfg.MaxConcurrentFwd {

			startCapacity := cfg.MaxConcurrentFwd - currentPricessingSize
			var nbrToStart int
			if currentQueueSize > startCapacity {
				nbrToStart = startCapacity
			} else {
				nbrToStart = currentQueueSize
			}

			for nbrToStart > 0 {
				var nbrItems int
				if maxNbrMessagesPolled < nbrToStart {
					nbrItems = maxNbrMessagesPolled
				} else {
					nbrItems = currentQueueSize
				}

				var msg = TriggerIndiElement{
					EndPointId: endPointId,
					NbrItems:   nbrItems,
				}

				fmt.Printf("forwarder.trigger.TriggerIndi() msg:%#v\n", msg)

				messageQueue <- &msg

				nbrToStart -= nbrItems

				// Update nbr processing for this endpoint
				nbr, err := forwarderRedis.IncrBy("FWD_IQ_PS_" + strconv.Itoa(endPointId), nbrItems)
				if nil != err {
					fmt.Printf("forwarder.trigger_indi.TriggerIndi() Failed to increase redis FWD_IQ_PS_%d by %d: %v\n", endPointId, nbrItems, err)
					break
				}
				fmt.Printf("forwarder.trigger_indi.TriggerIndi() FWD_IQ_PS_%d increased by %d and set to %d\n", endPointId, nbrItems, nbr)

				// update queue size for this endpoint. We count messages either as processing or on queue.
				nbr, err = forwarderRedis.IncrBy("FWD_IQ_QS_" + strconv.Itoa(endPointId), 0 - nbrItems)
				if nil != err {
					fmt.Printf("forwarder.trigger_indi.TriggerIndi() Failed to decrease redis FWD_IQ_QS_%d by %d: %v\n", endPointId, nbrItems, err)
					break
				}
				fmt.Printf("forwarder.trigger_indi.TriggerIndi() FWD_IQ_QS_%d increased by %d and set to %d\n", endPointId, nbrItems, nbr)
			}
		}
	}

	for j:=0; j<nbrPublishWorkers; j++ {
		messageQueue <- nil
	}

	waitGroup.Wait()

	fmt.Printf("forwarder.trigger_indi.TriggerIndi(%s) done! v%s Memstats: %s\n", devprod, forwarderCommon.PackageVersion, forwarderStats.GetMemUsageStr())

	return nil
}
