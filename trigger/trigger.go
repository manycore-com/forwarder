package trigger

import (
	"context"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"math"
	"os"
	"strconv"
	"sync"
)

// Still used by Trigger Fanout

var maxNbrMessagesPolled = 64  // This should be the same in forwarder!
var subscriptionToProcess = ""
var triggerTopicId = ""
var triggerSubscriptionId = ""
var projectId = ""
var devprod = ""
var nbrPublishWorkers = 32
var triggerPackagesScaler float64 = 1.0  // Somehow it seems like we don't consume everything, so let's try surplus triggers
func env() error {
	var err error

	projectId = os.Getenv("PROJECT_ID")
	devprod = os.Getenv("DEV_OR_PROD")


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

	subscriptionToProcess = os.Getenv("SUBSCRIPTION_TO_PROCESS")
	if "" == subscriptionToProcess {
		return fmt.Errorf("mandatory SUBSCRIPTION_TO_PROCESS environment variable missing")
	}

	triggerTopicId = os.Getenv("TRIGGER_TOPIC")
	if "" == triggerTopicId {
		return fmt.Errorf("mandatory TRIGGER_TOPIC environment variable missing")
	}

	triggerSubscriptionId = os.Getenv("TRIGGER_SUBSCRIPTION_ID")
	if "" == triggerSubscriptionId {
		return fmt.Errorf("mandatory TRIGGER_SUBSCRIPTION_ID environment variable missing")
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

func asyncSendTriggerPackages(channel *chan int64, waitGroup *sync.WaitGroup, triggerTopicId string, nbrWorkers int) {
	for i := 0; i < nbrWorkers; i++ {
		waitGroup.Add(1)

		go func(idx int, waitGroup *sync.WaitGroup) {
			defer waitGroup.Done()

			ctx, client, triggerTopic, err := forwarderPubsub.SetupClientAndTopic(projectId, triggerTopicId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				fmt.Printf("forwarder.fanout.asyncFanout(%s,%d): Critical Error: Failed to instantiate Topic Client: %v\n", devprod, idx, err)
				return
			}

			for {
				var val int64 = <-*channel
				if -1 == val {
					//fmt.Printf("forwarder.trigger.asyncSendTriggerPackages(%s,%d): trigger sender worker done\n", devprod, idx)
					return
				}

				var msg = fmt.Sprintf("{\"tp\":%d}", val)
				err := forwarderPubsub.PushAndWaitJsonStringToPubsub(ctx, triggerTopic, msg)
				if err != nil {
					fmt.Printf("forwarder.fanout.asyncFanout(%s,%d): Failed to publish to trigger topic %s: %v\n", devprod, idx, triggerTopicId, err)
				}
			}

		}(i, waitGroup)
	}

}

func cleanup() {
	forwarderStats.CleanupV2()
}

func Trigger(ctx context.Context, m forwarderPubsub.PubSubMessage) error {
	defer cleanup()

	err := env()
	if nil != err {
		return fmt.Errorf("forwarder.trigger.Trigger() is mis configured: %v", err)
	}

	fmt.Printf("forwarder.trigger.Trigger(%s) Entry: Memstats: %s\n", devprod, forwarderStats.GetMemUsageStr())

	// 1. How many items are already on the trigger queue?
	alreadyOnTriggerQueue, err := forwarderPubsub.CheckNbrItemsPubsub(projectId, triggerSubscriptionId)
	if nil != err {
		fmt.Printf("forwarder.trigger.Trigger() Failed to check trigger queue size proj:%s subscription:%s: %v\n", projectId, triggerSubscriptionId, err)
		return err
	}
	fmt.Printf("forwarder.trigger.Trigger() Size of trigger queue: proj:%s subscription:%s size:%d\n", projectId, triggerSubscriptionId, alreadyOnTriggerQueue)

	// 2. Figure out how many items there in the subscription
    nbrItemsInt64, err := forwarderPubsub.CheckNbrItemsPubsub(projectId, subscriptionToProcess)
	if nil != err {
		fmt.Printf("forwarder.trigger.Trigger() Failed to check queue size proj:%s subscription:%s: %v\n", projectId, subscriptionToProcess, err)
		return err
	}
	fmt.Printf("forwarder.trigger.Trigger() Size of message queue: subscription:%s size:%d\n", subscriptionToProcess, nbrItemsInt64)


	fmt.Printf("forwarder.trigger.Trigger(%s) After size checks: Memstats: %s\n", devprod, forwarderStats.GetMemUsageStr())

	iterations := int64(math.Ceil(triggerPackagesScaler * float64(nbrItemsInt64) / float64(maxNbrMessagesPolled))) - alreadyOnTriggerQueue

	// 3. Send the trigger packages in concurrently or we'll be here all day.
	messageQueue := make(chan int64, nbrPublishWorkers)
	defer close(messageQueue)
	var waitGroup sync.WaitGroup

	asyncSendTriggerPackages(&messageQueue, &waitGroup, triggerTopicId, nbrPublishWorkers)

	var i int64
	for i=0; i<iterations; i++ {
		messageQueue <- i
	}

	// 3. Stop the async senders
	for j:=0; j<nbrPublishWorkers; j++ {
		messageQueue <- int64(-1)
	}

	waitGroup.Wait()

	fmt.Printf("forwarder.trigger.Trigger(%s) done! v%s msg on queue: %v, trig msg already:%v, new trigger msg:%v, Memstats: %s\n", devprod, forwarderCommon.PackageVersion, nbrItemsInt64, alreadyOnTriggerQueue, iterations, forwarderStats.GetMemUsageStr())

	return nil
}
