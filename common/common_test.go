package common

import (
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSendMail(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := Env()
	assert.NoErrorf(t, err, "Failed to init variables")

	err = SendMail(GetDefaultFrom(os.Getenv("FORWARDER_EMAIL_REPLY_TO")), os.Getenv("FORWARDER_EMAIL_REPLY_TO"), []string{os.Getenv("FORWARDER_EMAIL_TO")}, nil, []string{"ehsmeng@bluewin.ch"},"test4", "and the magnificent message")
	assert.NoError(t, err, "fail")
}
