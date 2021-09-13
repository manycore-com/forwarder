package forward_indi

import (
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUpdateResendData(t *testing.T) {

	cleanup()
	updateResendData("age0", "count0", int64(100))
	updateResendData("age0", "count0", int64(99))
	updateResendData("age0", "count0", int64(101))

	updateResendData("age1", "count1", int64(500))

	assert.Equal(t, int64(99), redisKeyToLowestEpoch["age0"])
	assert.Equal(t, int64(500), redisKeyToLowestEpoch["age1"])

	assert.Equal(t, 3, redisKeyToNbrMsg["count0"])
	assert.Equal(t, 1, redisKeyToNbrMsg["count1"])

	cleanup()
}

func TestWarnIfOld(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := env()
	assert.NoError(t, err)

	cleanup()
	defer cleanup()

	err = forwarderRedis.Init()
	assert.NoError(t, err)
	defer forwarderRedis.Cleanup()

	elem := forwarderPubsub.PubSubElement{
		CompanyID: 1,
		ESP: "sg",
		ESPJsonString: "{\"apa\":1}",
		Ts: time.Now().Unix() - 30 * 3600,
		EndPointId: 1,
	}

	key := fmt.Sprintf("WARNED_24H_OR_48h_%d", elem.EndPointId)

	// Delete possible key
	err = forwarderRedis.Del(key)
	assert.NoError(t, err)

	aMap := map[int]*forwarderPubsub.PubSubElement{}
	aMap[elem.EndPointId] = &elem

	val := warnIfOld(aMap)
	assert.Equal(t, 1, val)

	val, err = forwarderRedis.GetInt(key)
	assert.NoError(t, err)
	assert.Equal(t, 24, val)

	val = warnIfOld(aMap)
	assert.Equal(t, 0, val)

	val, err = forwarderRedis.GetInt(key)
	assert.NoError(t, err)
	assert.Equal(t, 24, val)



	elem = forwarderPubsub.PubSubElement{
		CompanyID: 1,
		ESP: "sg",
		ESPJsonString: "{\"apa\":1}",
		Ts: time.Now().Unix() - 50 * 3600,
		EndPointId: 1,
	}

	aMap = map[int]*forwarderPubsub.PubSubElement{}
	aMap[elem.EndPointId] = &elem

	val = warnIfOld(aMap)
	assert.Equal(t, 1, val)

	val, err = forwarderRedis.GetInt(key)
	assert.NoError(t, err)
	assert.Equal(t, 48, val)


	val = warnIfOld(aMap)
	assert.Equal(t, 0, val)

	val, err = forwarderRedis.GetInt(key)
	assert.NoError(t, err)
	assert.Equal(t, 48, val)
}