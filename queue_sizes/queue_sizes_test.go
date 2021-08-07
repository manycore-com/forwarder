package queue_sizes

import (
	"fmt"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestCalculateCurrentQueueSize(t *testing.T) {
	forwarderTest.SetEnvVars()
	assert.NoErrorf(t, forwarderRedis.Init(), "Redis init failed")
	defer forwarderRedis.Cleanup()

	var resendQueueTemplates = []string{
		"INBOXBOOSTER_DEV_FORWARD2_%d",
		"INBOXBOOSTER_DEV_FORWARD3_%d",
		"INBOXBOOSTER_DEV_FORWARD4_%d",
	}
	a,b,c,err := CalculateCurrentQueueSize(1, resendQueueTemplates)
	assert.NoError(t, err, "CalculateCurrentQueueSize failed")
	fmt.Printf("resend size:%d, qs:%d, ps:%d\n", a, b, c)
}

func TestGetOldestAgeInResend(t *testing.T) {
	forwarderTest.SetEnvVars()
	assert.NoErrorf(t, forwarderRedis.Init(), "Redis init failed")
	defer forwarderRedis.Cleanup()

	var now = time.Now().Unix()

	assert.NoError(t, forwarderRedis.SetInt64("oldest_RalfRektor3_1", now - 1000), "problems")

	var fakeSubsIdTemplates = []string{
		"RalfRektor%d",
	}
	age, err := GetOldestAgeInResend(1, fakeSubsIdTemplates)
	fmt.Printf("age: %d\n", age)
	assert.NoError(t, err, "more problems")
	assert.True(t, age >= 999 && age <= 1001, "age wrong: " + strconv.Itoa(age))

	forwarderRedis.Del("oldest_RalfRektor3_1")
}

