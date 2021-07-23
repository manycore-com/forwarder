package individual_queues

import (
	"context"
	"encoding/json"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"sync"
)

// Moves packages from the resend queue to the forward queues.

func ResendIndi(ctx context.Context, m forwarderPubsub.PubSubMessage, destSubscriptionTemplate string) error {

	err := Env()
	if nil != err {
		fmt.Printf("forwarder.IQ.ResendIndi(): v%s Failed to setup Env: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer Cleanup()

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.IQ.ResendIndi(): v%s Failed to init Redis: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer forwarderRedis.Cleanup()

	// Setup writer to destination
	writerChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(writerChan)
	var writerWaitGroup sync.WaitGroup

	asyncWriterToIndividualQueues(&writerChan, &writerWaitGroup, destSubscriptionTemplate)

	var trgmsg TriggerResendElement
	err = json.Unmarshal(m.Data, &trgmsg)
	if nil != err {
		return fmt.Errorf("forwarder.forward_indi.ResendIndi() v%s Error decoding trigger message: %v", forwarderCommon.PackageVersion, err)
	}

	receivedInTotal, err := forwarderPubsub.ReceiveEventsFromPubsub(projectId, trgmsg.SubscriptionId, 0, int(trgmsg.NbrItems), &writerChan, maxPubsubQueueIdleMs, maxOutstandingMessages)
	if nil != err {
		// Super important too.
		fmt.Printf("forwarder.forward_indi.ForwardIndi(): v%s failed to receive events: %v\n", forwarderCommon.PackageVersion, err)
	}

	fmt.Printf("forwarder.forward_indi.ForwardIndi(): v%s received %d messages\n", forwarderCommon.PackageVersion, receivedInTotal)

	for i:=0; i<nbrPublishWorkers; i++ {
		writerChan <- nil
	}

	writerWaitGroup.Wait()

	fmt.Printf("forwarder.IQ.ResendIndi() done! v%s Memstats: %s\n", forwarderCommon.PackageVersion, forwarderStats.GetMemUsageStr())

	return nil
}