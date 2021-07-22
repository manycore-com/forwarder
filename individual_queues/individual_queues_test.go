package individual_queues

import (
	"context"
	"fmt"
	forwarderPause "github.com/manycore-com/forwarder/pause"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
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

func takeDownAsync(nbrWorkers int, writeBackWaitGroup *sync.WaitGroup, writeBackChan *chan *forwarderPubsub.PubSubElement) {
	for i:=0; i<nbrWorkers; i++ {
		*writeBackChan <- nil
	}

	writeBackWaitGroup.Wait()
}

func TestMoveToIndividual(t *testing.T) {
	forwarderTest.SetEnvVars()
	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("GCP_LOCATION", os.Getenv("FORWARDER_TEST_GCP_LOCATION"))
	os.Setenv("NBR_HASH", "1")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GAE_CREDENTIALS_JOBS"))

	forwarderRedis.Init()

	err := Env()
	assert.NoError(t, err, "So sad now")
	err = forwarderPause.Env()
	assert.NoError(t, err, "So sad now2")

	nbrWriteBackWorkers := 20

	// Put some shit on TESTING and TESTING2
	// Setup writeback queue
	writeBackChan := make(chan *forwarderPubsub.PubSubElement, nbrWriteBackWorkers)
	defer close(writeBackChan)
	var writeBackWaitGroup sync.WaitGroup

	// Starts async writers
	forwarderPause.WriteBackMessages(nbrWriteBackWorkers, &writeBackChan, &writeBackWaitGroup, "TESTING")

	for i:=0; i<10; i++ {
		m := forwarderPubsub.PubSubElement{
			CompanyID: rand.Intn(4) + 1,
			Ts: 123,
			EndPointId: rand.Intn(4) + 1,
			Rid: int(rand.Int31()),
		}

		writeBackChan <- &m
	}

	takeDownAsync(nbrWriteBackWorkers, &writeBackWaitGroup, &writeBackChan)

	// Starts async writers
	forwarderPause.WriteBackMessages(nbrWriteBackWorkers, &writeBackChan, &writeBackWaitGroup, "TESTING2")

	for i:=0; i<10; i++ {
		m := forwarderPubsub.PubSubElement{
			CompanyID: rand.Intn(4) + 1,
			Ts: 123,
			EndPointId: rand.Intn(4) + 1,
			Rid: int(rand.Int31()),
		}

		writeBackChan <- &m
	}

	takeDownAsync(nbrWriteBackWorkers, &writeBackWaitGroup, &writeBackChan)

	fmt.Printf("Sleeping 30s before trying to poll the lot")
	time.Sleep(time.Second * 30)

	err = MoveToIndividual(context.Background(), forwarderPubsub.PubSubMessage{}, []string{"TESTING", "TESTING2"}, "INBOXBOOSTER_DEVPROD_FORWARD_INDI_%d")
	assert.NoError(t, err, "seriously wrong")

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
