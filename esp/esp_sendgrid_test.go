package esp

import (
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestForwardSg(t *testing.T) {

	x := forwarderPubsub.PubSubElement{
		CompanyID: 1,
		ESP: "sg",
		ESPJsonString: "{\"apa\":1}",
		Ts: 123,
		Dest:"https://monitor.inboxbooster.com/webhook-forwarder/debug-post/",
	}

	fmt.Printf("e: %v\n", x)

	err, _ := ForwardSg("dev", &x)
	assert.NoErrorf(t, err, "failed to send")
}