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
	Env()
	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()
	defer Cleanup()

	var x map[int]string = map[int]string{
		1: "TESTING",
	}

	err := reCalculateUsersQueueSizes_(x)
	assert.NoError(t, err, "error")
}

func TestTouchForwardId(t *testing.T) {
	forwarderTest.SetEnvVars()
	forwarderRedis.Init()
	defer forwarderRedis.Cleanup()
	defer Cleanup()
	defer forwarderRedis.Del("FWD_IQ_ACTIVE_ENDPOINTS_SET")

	forwarderRedis.Del("FWD_IQ_ACTIVE_ENDPOINTS_SET")
	err := TouchForwardId(2)
	assert.NoError(t, err, "dang it")
	err = TouchForwardId(7)
	assert.NoError(t, err, "oh no")
	err = TouchForwardId(2)
	assert.NoError(t, err, "bo ho")

	x, err := forwarderRedis.SetMembersInt("FWD_IQ_ACTIVE_ENDPOINTS_SET")
	assert.NoError(t, err, "failed to get set")
	assert.Equal(t, 2, len(x))
}

func TestX(t *testing.T) {
	forwarderTest.SetEnvVars()
	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	forwarderRedis.Init()

	err := Env()
	assert.NoError(t, err, "So sad now")


}
