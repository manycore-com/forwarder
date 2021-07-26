package individual_queues

import (
	"context"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"sync"
)

type TriggerResendElement struct {
	SubscriptionId  string
	NbrItems        int64
}


func asyncSendResendTriggerPackages(channel *chan *TriggerResendElement, waitGroup *sync.WaitGroup, nbrWorkers int, triggerTopicId string) {
	for i := 0; i < nbrWorkers; i++ {
		waitGroup.Add(1)

		go func(idx int, waitGroup *sync.WaitGroup) {
			defer waitGroup.Done()

			ctx, client, err := forwarderPubsub.SetupClient(projectId)
			if nil != client {
				defer client.Close()
			}

			if err != nil {
				fmt.Printf("forwarder.IQ.asyncSendResendTriggerPackages(%d): Critical Error: Failed to instantiate Topic Client: %v\n", idx, err)
				return
			}

			outTopic := client.Topic(triggerTopicId)

			for {
				var msg = <- *channel
				if nil == msg {
					//fmt.Printf("forwarder.trigger.asyncSendTriggerPackages(%s,%d): trigger sender worker done\n", devprod, idx)
					return
				}

				var trgmsg = fmt.Sprintf("{\"SubscriptionId\":\"%s\", \"NbrItems\":%d}", msg.SubscriptionId, msg.NbrItems)

				err := forwarderPubsub.PushAndWaitJsonStringToPubsub(ctx, outTopic, trgmsg)
				if err != nil {
					fmt.Printf("forwarder.IQ.asyncSendResendTriggerPackages(%d): Failed to publish to trigger topic %s: %v\n", idx, triggerTopicId, err)
				} else {
					fmt.Printf("forwarder.IQ.asyncSendResendTriggerPackages() success: %#v\n", msg)
				}
			}

		}(i, waitGroup)
	}
}


func TriggerResend(ctx context.Context, m forwarderPubsub.PubSubMessage, subscriptionIds []string, triggerTopicId string) error {

	err := Env()
	if nil != err {
		fmt.Printf("forwarder.IQ.TriggerResend(): v%s Failed to setup Env: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.IQ.TriggerResend(): v%s Failed to init Redis: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer forwarderRedis.Cleanup()

	// Launch the async pubsub writer
	messageQueue := make(chan *TriggerResendElement, nbrPublishWorkers)
	defer close(messageQueue)
	var waitGroup sync.WaitGroup

	asyncSendResendTriggerPackages(&messageQueue, &waitGroup, nbrPublishWorkers, triggerTopicId)


	subsToCount, err := forwarderPubsub.CheckNbrItemsPubsubs(projectId, subscriptionIds)
	if nil != err {
		return fmt.Errorf("forwarder.IQ.TriggerResend() Failed to check queue sizes: %v", err)
	}

	for _, subscriptionId := range subscriptionIds {

		if val, ok := subsToCount[subscriptionId]; ok {

			for val > 0 {

				var nbrItems = int64(0)
				if val > int64(maxNbrMessagesPolled) {
					nbrItems = int64(maxNbrMessagesPolled)
				} else {
					nbrItems = val
				}

				msg := TriggerResendElement{
					SubscriptionId: subscriptionId,
					NbrItems: nbrItems,
				}

				messageQueue <- &msg

				val -= nbrItems
			}

		}
	}


	// Take down the pubsub writer
	for j:=0; j<nbrPublishWorkers; j++ {
		messageQueue <- nil
	}

	waitGroup.Wait()

	fmt.Printf("forwarder.IQ.TriggerResend() done! v%s Memstats: %s\n", forwarderCommon.PackageVersion, forwarderStats.GetMemUsageStr())

	return nil
}
