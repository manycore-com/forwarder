package esp

import (
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestForwardSg(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := forwarderRedis.Init()
	assert.NoError(t, err, "Fail")
	defer forwarderRedis.Cleanup()

	for i:=0; i<300; i++ {
		x := forwarderPubsub.PubSubElement{
			CompanyID: 1,
			ESP: "sg",
			ESPJsonString: "{\"apa\":1}",
			Ts: 123,
			EndPointId: 1,
		}

		fmt.Printf("e: %v\n", x)

		err, _ := ForwardSg("dev", &x)
		assert.NoErrorf(t, err, "failed to send")

		fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())
	}


}