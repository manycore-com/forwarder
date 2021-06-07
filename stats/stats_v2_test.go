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

	AddReceivedAtH(123)
	AddReceivedAtH(1234)
	assert.Equal(t, 2, AddReceivedAtH(123))
}
