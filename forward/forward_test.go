package forward

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestEnv(t *testing.T) {
	os.Setenv("DEV_OR_PROD", "dev")
	os.Setenv("PROJECT_ID", "aproj")
	os.Setenv("AT_QUEUE", "1")
	os.Setenv("IN_SUBSCRIPTION_ID", "fwd1")
	os.Setenv("OUT_QUEUE_TOPIC_ID", "fwd2")

	os.Setenv("DB_USER", "apa")
	os.Setenv("DB_PASS", "banan")
	os.Setenv("INSTANCE_CONNECTION_NAME", "citron")
	os.Setenv("DB_NAME", "dadel")

	os.Setenv("MIN_AGE_SECS", "0")
	os.Setenv("NBR_ACK_WORKER", "32")
	os.Setenv("NBR_PUBLISH_WORKER", "32")
	os.Setenv("MAX_NBR_MESSAGES_POLLED", "64")
	os.Setenv("MAX_PUBSUB_QUEUE_IDLE_MS", "500")

	env()

	assert.Equal(t, 500, maxPubsubQueueIdleMs, "maxPubsubQueueIdleMs is supposed to be 500")
}
