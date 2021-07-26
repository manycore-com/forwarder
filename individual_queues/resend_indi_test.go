package individual_queues

import (
	"context"
	"encoding/json"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestUnmarshal(t *testing.T) {

	var jsonstr = `
        {"SubscriptionId":"TESTING", "NbrItems":2}
    `
	var trgmsg TriggerResendElement
	err := json.Unmarshal([]byte(jsonstr), &trgmsg)
	assert.NoErrorf(t, err, "oh! pity!")

}

func TestResendIndi(t *testing.T) {

	forwarderTest.SetEnvVars()
	Env()

	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()
	defer Cleanup()

	var jsonmessage = `{
    "SubscriptionId": "TESTING", "NbrItems": 1 
    }`


	var m = forwarderPubsub.PubSubMessage{
		Data: []byte(jsonmessage),
	}

	ResendIndi(context.Background(), m, "INBOXBOOSTER_DEVPROD_FORWARD_INDI_%d")

}
