package trigger

import (
	"github.com/stretchr/testify/assert"
	"os"
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


