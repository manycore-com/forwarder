package pause

import (
	"fmt"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPause(t *testing.T) {
	forwarderTest.SetEnvVars()

	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	err := env()
	assert.NoError(t, err, "env() failed")

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

	err := env()
	assert.NoError(t, err, "env() failed")

	var jobNames []string = []string{"TriggerFanoutDevProd"}
	err = Resume(jobNames)
	assert.NoError(t, err, "Pause() failed")
}



func TestWriteBackMessages(t *testing.T) {
	rand.Seed(time.Now().UnixNano())  // Rid in PubSubElement also needs Seed

	nbrWriteBackWorkers := 4

	forwarderTest.SetEnvVars()
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GCP_LOCATION", "us-central1")

	err := env()
	assert.NoError(t, err, "env() failed")

	// Setup writeback queue
	writeBackChan := make(chan *forwarderPubsub.PubSubElement, nbrWriteBackWorkers)
	defer close(writeBackChan)
	var writeBackWaitGroup sync.WaitGroup

	// Starts async writers
	WriteBackMessages(nbrWriteBackWorkers, &writeBackChan, &writeBackWaitGroup, "TESTING")

	for i:=0; i<10; i++ {
		m := forwarderPubsub.PubSubElement{
			CompanyID: rand.Intn(4) + 1,
			Ts: 123,
			EndPointId: 1,
			Rid: int(rand.Int31()),
		}

		writeBackChan <- &m
	}

	func(nbrWorkers int, writeBackWaitGroup *sync.WaitGroup) {
		for i:=0; i<nbrWorkers; i++ {
			writeBackChan <- nil
		}

		writeBackWaitGroup.Wait()
	} (nbrWriteBackWorkers, &writeBackWaitGroup)


}

func TestMoveAndCount(t *testing.T) {
	forwarderTest.SetEnvVars()
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GCP_LOCATION", "us-central1")

	err := env()
	assert.NoError(t, err, "env() failed")

	apa := MoveAndCount([][]string{ []string{"TESTING", "TESTING2"}  }, false)
	assert.False(t, apa)
	for endpointId, count := range EndpointCountMap {
		fmt.Printf("endpoint: %4d, count: %4d\n", endpointId, count)
	}

}

func TestMoveAndCountReverse(t *testing.T) {
	forwarderTest.SetEnvVars()
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GCP_LOCATION", "us-central1")

	err := env()
	assert.NoError(t, err, "env() failed")

	apa := MoveAndCount([][]string{ []string{"TESTING", "TESTING2"}  }, true)
	assert.False(t, apa)
	for endpointId, count := range EndpointCountMap {
		fmt.Printf("endpoint: %4d, count: %4d\n", endpointId, count)
	}

}

func TestCountAndCheckpoint2(t *testing.T) {
	forwarderTest.SetEnvVars()
	companiesAndEndpoints, err := forwarderDb.GetLatestActiveEndpoints()
	assert.NoError(t, err, "Dang it")

	for _, companiesAndEndpoint := range companiesAndEndpoints {
		fmt.Printf("forwarder.pause.CountAndCheckpoint2() Writing active companies with nothing on resend queue. endpoint:%d\n", companiesAndEndpoint.EndPointId)
		err := forwarderDb.WriteQueueCheckpoint(companiesAndEndpoint.EndPointId, 0)
		assert.NoError(t, err, "Dang it")
	}
}
