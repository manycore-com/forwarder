package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSmth(t *testing.T) {
	assert.Equal(t, 2, epochThenToOffs(time.Now().Unix() - int64(7500)))
}

func TestAddReceivedAtH(t *testing.T) {
	var now = time.Now().UTC()
	if now.Minute() == 59 && now.Second() == 59 {
		time.Sleep(time.Second * 1)
	}

	AddEnterQueueAtH(1235)
	AddEnterQueueAtH(1235)
	AddEnterQueueAtH(1235)

	AddReceivedAtH(123)
	AddReceivedAtH(1234)

	assert.Equal(t, 2, AddReceivedAtH(123))
	assert.Equal(t, 4, AddEnterQueueAtH(1235))

	CleanupV2()
}

func TestTsToTruncOffs(t *testing.T) {
	var now = time.Now().Unix()
	assert.Equal(t, 0, TsToTruncOffs(now + int64(1000)))
	assert.Equal(t, 0, TsToTruncOffs(now - 3600 * 0))
	assert.Equal(t, 1, TsToTruncOffs(now - 3600 * 1))
	assert.Equal(t, 2, TsToTruncOffs(now - 3600 * 2))
	assert.Equal(t, 3, TsToTruncOffs(now - 3600 * 3))
	assert.Equal(t, 4, TsToTruncOffs(now - 3600 * 4))
	assert.Equal(t, 5, TsToTruncOffs(now - 3600 * 5))
	assert.Equal(t, 6, TsToTruncOffs(now - 3600 * 6))
	assert.Equal(t, 7, TsToTruncOffs(now - 3600 * 7))
	assert.Equal(t, 8, TsToTruncOffs(now - 3600 * 8))
	assert.Equal(t, 9, TsToTruncOffs(now - 3600 * 9))
	assert.Equal(t, 10, TsToTruncOffs(now - 3600 * 10))
	assert.Equal(t, 11, TsToTruncOffs(now - 3600 * 11))
	assert.Equal(t, 12, TsToTruncOffs(now - 3600 * 12))
	assert.Equal(t, 12, TsToTruncOffs(now - 3600 * 13))
	assert.Equal(t, 12, TsToTruncOffs(now - 3600 * 14))
	assert.Equal(t, 12, TsToTruncOffs(now - 3600 * 15))
	assert.Equal(t, 16, TsToTruncOffs(now - 3600 * 16))
	assert.Equal(t, 16, TsToTruncOffs(now - 3600 * 17))
	assert.Equal(t, 16, TsToTruncOffs(now - 3600 * 18))
	assert.Equal(t, 16, TsToTruncOffs(now - 3600 * 19))
	assert.Equal(t, 20, TsToTruncOffs(now - 3600 * 20))
	assert.Equal(t, 20, TsToTruncOffs(now - 3600 * 21))
	assert.Equal(t, 20, TsToTruncOffs(now - 3600 * 22))
	assert.Equal(t, 20, TsToTruncOffs(now - 3600 * 23))
	assert.Equal(t, 24, TsToTruncOffs(now - 3600 * 24))
	assert.Equal(t, 24, TsToTruncOffs(now - 3600 * 25))
	assert.Equal(t, 24, TsToTruncOffs(now - 3600 * 26))
	assert.Equal(t, 24, TsToTruncOffs(now - 3600 * 27))
	assert.Equal(t, 28, TsToTruncOffs(now - 3600 * 28))
	assert.Equal(t, 28, TsToTruncOffs(now - 3600 * 29))
	assert.Equal(t, 28, TsToTruncOffs(now - 3600 * 30))
	assert.Equal(t, 28, TsToTruncOffs(now - 3600 * 31))
	assert.Equal(t, 32, TsToTruncOffs(now - 3600 * 32))
	assert.Equal(t, 32, TsToTruncOffs(now - 3600 * 33))
	assert.Equal(t, 32, TsToTruncOffs(now - 3600 * 34))
	assert.Equal(t, 32, TsToTruncOffs(now - 3600 * 35))
	assert.Equal(t, 36, TsToTruncOffs(now - 3600 * 36))
	assert.Equal(t, 36, TsToTruncOffs(now - 3600 * 37))
	assert.Equal(t, 36, TsToTruncOffs(now - 3600 * 38))
	assert.Equal(t, 36, TsToTruncOffs(now - 3600 * 39))
	assert.Equal(t, 36, TsToTruncOffs(now - 3600 * 40))
	assert.Equal(t, 36, TsToTruncOffs(now - 3600 * 41))
	assert.Equal(t, 42, TsToTruncOffs(now - 3600 * 42))
	assert.Equal(t, 42, TsToTruncOffs(now - 3600 * 43))
	assert.Equal(t, 42, TsToTruncOffs(now - 3600 * 44))
	assert.Equal(t, 42, TsToTruncOffs(now - 3600 * 45))
	assert.Equal(t, 42, TsToTruncOffs(now - 3600 * 46))
	assert.Equal(t, 42, TsToTruncOffs(now - 3600 * 47))
	assert.Equal(t, 48, TsToTruncOffs(now - 3600 * 48))
	assert.Equal(t, 48, TsToTruncOffs(now - 3600 * 49))
	assert.Equal(t, 48, TsToTruncOffs(now - 3600 * 50))
	assert.Equal(t, 48, TsToTruncOffs(now - 3600 * 51))
	assert.Equal(t, 48, TsToTruncOffs(now - 3600 * 52))
	assert.Equal(t, 48, TsToTruncOffs(now - 3600 * 53))
	assert.Equal(t, 54, TsToTruncOffs(now - 3600 * 54))
	assert.Equal(t, 54, TsToTruncOffs(now - 3600 * 55))
	assert.Equal(t, 54, TsToTruncOffs(now - 3600 * 56))
	assert.Equal(t, 54, TsToTruncOffs(now - 3600 * 57))
	assert.Equal(t, 54, TsToTruncOffs(now - 3600 * 58))
	assert.Equal(t, 54, TsToTruncOffs(now - 3600 * 59))
	assert.Equal(t, 60, TsToTruncOffs(now - 3600 * 60))
	assert.Equal(t, 60, TsToTruncOffs(now - 3600 * 61))
	assert.Equal(t, 60, TsToTruncOffs(now - 3600 * 62))
	assert.Equal(t, 60, TsToTruncOffs(now - 3600 * 63))
	assert.Equal(t, 60, TsToTruncOffs(now - 3600 * 64))
	assert.Equal(t, 60, TsToTruncOffs(now - 3600 * 65))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 66))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 67))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 68))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 69))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 70))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 71))
	assert.Equal(t, 66, TsToTruncOffs(now - 3600 * 72))
}
