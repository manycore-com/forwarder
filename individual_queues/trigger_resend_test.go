package individual_queues

import (
	"context"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTriggerResend(t *testing.T) {
	forwarderTest.SetEnvVars()
	Env()
	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()
	defer Cleanup()

	var x []string = []string{
		"TESTING",
	}

	err := TriggerResend(context.Background(), forwarderPubsub.PubSubMessage{}, x, "TESTING2")
	assert.NoError(t, err, "error")
}

