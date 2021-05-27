package esp

import (
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestForwardSg(t *testing.T) {

	for i:=0; i<300; i++ {
		x := forwarderPubsub.PubSubElement{
			CompanyID: 1,
			ESP: "sg",
			ESPJsonString: "{\"apa\":1}",
			Ts: 123,
			Dest:"http://localhost:8000/webhook-forwarder/debug-post/",
		}

		fmt.Printf("e: %v\n", x)

		err, _ := ForwardSg("dev", &x)
		assert.NoErrorf(t, err, "failed to send")

		fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())
	}


}