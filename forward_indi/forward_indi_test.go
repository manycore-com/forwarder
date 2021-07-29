package forward_indi

import (
	"github.com/stretchr/testify/assert"
	"testing"
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

