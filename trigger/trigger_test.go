package trigger

import (
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestAsyncSendTriggerPackages(t *testing.T) {

	assert.NotEmpty(t, os.Getenv("FORWARDER_TEST_QUEUE_1"), "FORWARDER_TEST_QUEUE_1 environment variable missing")

	os.Setenv("DEV_OR_PROD", "dev")
	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("SUBSCRIPTION_TO_TRIGGER", os.Getenv("FORWARDER_TEST_QUEUE_1"))
	os.Setenv("TRIGGER_TOPIC", os.Getenv("FORWARDER_TEST_TRIGGER_1"))
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GOOGLE_APPLICATION_CREDENTIALS"))

	err := env()
	assert.NoError(t, err, "Gosh darn it, env() failed")

	iterations := int64(1)

	// 2. Send the trigger packages in concurrently or we'll be here all day.
	messageQueue := make(chan int64, nbrPublishWorkers)
	defer close(messageQueue)
	var waitGroup sync.WaitGroup

	assert.NotEmpty(t, triggerTopicId, "triggerTopicId is empty")

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
}

func apa(nbrPublishWorkers int, triggerTopicId string) {
	messageQueue := make(chan int64, nbrPublishWorkers)
	defer close(messageQueue)
	var waitGroup sync.WaitGroup

	asyncSendTriggerPackages(&messageQueue, &waitGroup, triggerTopicId, nbrPublishWorkers)


	// 3. Stop the async senders
	for j:=0; j<nbrPublishWorkers; j++ {
		messageQueue <- int64(-1)
	}

	waitGroup.Wait()
}

func TestXX(t *testing.T) {

	fmt.Printf("mem start: %s\n", forwarderStats.GetMemUsageStr())

	for i:=0; i<30; i++ {
		apa(32, os.Getenv("FORWARDER_TEST_RESPONDER_TRG"))
		fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())
	}

}

func t123() error {
	var err error
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

	return nil
}

func TestYY(t *testing.T) {
	os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")

	e := t123()
	fmt.Printf("ant: %v, er:%v \n", maxNbrMessagesPolled, e)

}

func TestBugNotSeeingItems(t *testing.T) {
	forwarderTest.SetEnvVars()

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/home/ehsmeng/ib3keys/gcloud_credentials/inboxbooster3-4acfb5d777ed-pubsub-devprod-queue.json")

	env()
	subscriptionToProcess = "INBOXBOOSTER_DEVPROD_FORWARD3_2"

	nbrItemsInt64, err := forwarderPubsub.CheckNbrItemsPubsub(projectId, subscriptionToProcess)
	assert.NoError(t, err, "Failed to check")
	fmt.Printf("nbr elem: %d\n", nbrItemsInt64)
}

