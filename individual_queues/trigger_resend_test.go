package individual_queues

import (
	"context"
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestTriggerResend(t *testing.T) {
	forwarderTest.SetEnvVars()
	Env()

	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()
	defer Cleanup()

	m := forwarderPubsub.PubSubElement{
		CompanyID: rand.Intn(4) + 1,
		Ts: 123,
		EndPointId: 1,
		Rid: int(rand.Int31()),
	}

	ctx1, client, topic, err := forwarderPubsub.SetupClientAndTopic(projectId, "TESTING")
	if err != nil {
		fmt.Printf("forwarder.pause.writeBackMessages(%s): Critical Error: Failed to instantiate Client: %v\n", "TESTING", err)
		return
	}

	if nil != client {
		defer client.Close()
	}

	err = forwarderPubsub.PushAndWaitElemToPubsub(ctx1, topic, &m)
	if nil != err {
		fmt.Printf("forwarder.pause.writeBackMessages(%s): error pushing: %v\n", "TESTING", err)
		return
	}

	fmt.Printf("sleeping 30s to give the subscription a chance to register the item.\n")
	//time.Sleep(time.Second * 30)

	var x []string = []string{
		"TESTING",
	}

	err = TriggerResend(context.Background(), forwarderPubsub.PubSubMessage{}, x, "DEVNULL")
	assert.NoError(t, err, "error")
}

