package pause

import (
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPause(t *testing.T) {
	forwarderTest.SetEnvVars()

	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	err := Env()
	assert.NoError(t, err, "Env() failed")

	var jobNames []string = []string{"TriggerFanoutDevProd"}
	err = Pause(jobNames)
	assert.NoError(t, err, "Pause() failed")
}

func TestResume(t *testing.T) {
	forwarderTest.SetEnvVars()

	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	err := Env()
	assert.NoError(t, err, "Env() failed")

	var jobNames []string = []string{"TriggerFanoutDevProd"}
	err = Resume(jobNames)
	assert.NoError(t, err, "Pause() failed")
}

