package fanout

import (
	"context"
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCalculateHashFromSecret(t *testing.T) {
	fmt.Printf("local hash: %s\n", CalculateSafeHashFromSecret("0f33e12b-97f1-40b0-9ec7-7a86941886a9"))
}

func TestAsyncFanout(t *testing.T) {

	forwarderTest.SetEnvVars()
	os.Setenv("IN_SUBSCRIPTION_ID", "x")
	os.Setenv("OUT_QUEUE_TOPIC_ID", os.Getenv("FORWARDER_TEST_QUEUE_1"))
	env()

	pubsubForwardChan := make(chan *forwarderPubsub.PubSubElement, 2000)
	defer close(pubsubForwardChan)
	var forwardWaitGroup sync.WaitGroup

	asyncFanout(&pubsubForwardChan, &forwardWaitGroup)

	const okPayload = `[
    {
      "email":"apa4@banan.com",
      "timestamp":1576683110,
      "smtp-id":"<14c5d75ce93.dfd.64b469@ismtpd-555>",
      "event":"delivered",
      "category":"cat facts",
      "marketing_campaign_id":"supercampaign",
      "sg_event_id":"sg_event_id",
      "sg_message_id":"sg_message_id"
    }
]`
	var x = forwarderPubsub.PubSubElement{CompanyID: 1, ESP: "sg", ESPJsonString: okPayload, Ts: time.Now().Unix(), SafeHash: "09e0190b36476d1a960669bdb1dd0111", Sign: "", Dest: ""}
	pubsubForwardChan <- &x

	takeDownAsyncFanout(&pubsubForwardChan, &forwardWaitGroup)
}

func TestFanout(t *testing.T) {

	forwarderTest.SetEnvVars()
	os.Setenv("IN_SUBSCRIPTION_ID", os.Getenv("FORWARDER_TEST_RESPONDER_SUBS"))
	os.Setenv("OUT_QUEUE_TOPIC_ID", os.Getenv("FORWARDER_TEST_QUEUE_1"))
	env()

	var m = forwarderPubsub.PubSubMessage{Data: []byte("")}
	Fanout(context.Background(), m)
}
