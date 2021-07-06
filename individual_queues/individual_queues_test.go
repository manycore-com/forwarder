package individual_queues

import (
	"fmt"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func TestGetEndPointData(t *testing.T) {
	forwarderTest.SetEnvVars()
	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()

	defer Cleanup()

	forwarderRedis.Del("FWD_IQ_GETENDPOINTDATA_" + strconv.Itoa(1))

	x, err := GetEndPointData(1)
	assert.NoError(t, err, "fail")
	fmt.Printf("%#v\n", x)

	Cleanup()

	x, err = GetEndPointData(1)
	assert.NoError(t, err, "fail")
	fmt.Printf("%#v\n", x)

	x, err = GetEndPointData(1)
	assert.NoError(t, err, "fail")
	fmt.Printf("%#v\n", x)

}

func TestReCalculateUsersQueueSizes(t *testing.T) {
	forwarderTest.SetEnvVars()
	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()
	defer Cleanup()

	var x map[int]string = map[int]string{
		1: "TESTING",
	}

	err := ReCalculateUsersQueueSizes(os.Getenv("PROJECT_ID"), x)
	assert.NoError(t, err, "error")
}
