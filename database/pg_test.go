package forwarderDb

import (
	"fmt"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetUserData2(t *testing.T) {

	forwarderTest.SetEnvVars()

	ci, err := GetUserData(1)
	if err != nil {
		t.Errorf("failed to get user data: %v", err)
	} else {
		fmt.Printf("user data: %#v\n", *ci)
	}

	assert.NotEmpty(t, ci.Secret, "Missing secret!")

	ci, err = GetUserData(11111)
	if err != nil {
		t.Errorf("It should return nil silently if no row found: %v", err)
	} else {
		if ci != nil {
			t.Errorf("test invalid. Didn't really expect a row: %v", *ci)
		}

	}
}

func TestGetUserData(t *testing.T) {
	forwarderTest.SetEnvVars()

	ci, err := GetUserData(1)
	assert.NoError(t, err, "Failed to get user data")
	fmt.Printf("User data: %#v\n", *ci)
}

func TestCheckDb(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := CheckDb()
	assert.NoErrorf(t, err, "CheckDb failed")
}

func TestWriteStatsToDbV2(t *testing.T) {
	endpointId := 2

	forwarderTest.SetEnvVars()

	forwarderStats.AddForwardedAtH(1, endpointId)
	forwarderStats.AddErrorMessage(1,endpointId, "klaskatt3")
	forwarderStats.AddExample(1, endpointId, "some example3")
	forwarderStats.AddLost(1, endpointId)
	forwarderStats.AddLost(1, endpointId)
	forwarderStats.AddLost(1, endpointId)
	forwarderStats.AddTimeout(1, endpointId)
	forwarderStats.AddTimeout(1, endpointId)

	var now = time.Now().Unix()
	forwarderStats.AddAgeWhenForward(1, endpointId, now - int64(60*3600))

	WriteStatsToDb()
}

func TestCreatePauseRows(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := CreatePauseRows(2)
	assert.NoError(t, err, "Failed to create Pause rows")

	assert.True(t, IsPaused(0), "seems like CreatePauseRow failed to create row for hashid 0")
	assert.True(t, IsPaused(1), "seems like CreatePauseRow failed to create row for hashid 1")
	assert.False(t, IsPaused(2), "seems like CreatePauseRow created row for hashid 2 even if noone asked him for it")

	err = DeleteAPauseRow(1)
	assert.NoError(t, err, "Failed to delete pause row with hash_id=1")
	assert.True(t, IsPaused(0), "seems like DeleteAPauseRow managed to remove row for hashid 0 even if we did not ask it to")
	assert.False(t, IsPaused(1), "seems like DeleteAPauseRow failed to remove row for hashid 1")

	err = DeleteAllPauseRows()
	assert.NoError(t, err, "Failed to wipe out all pause rows")
	assert.False(t, IsPaused(1), "seems like DeleteAllPauseRows failed to remove row for hashid 0")
}


func TestGetLatestActiveCompanies(t *testing.T) {
	forwarderTest.SetEnvVars()
	companies, err := GetLatestActiveCompanies()
	assert.NoError(t, err, "GetLatestActiveCompanies is buggy")
	assert.NotNil(t, companies, "Companies is nil!")
}

func TestGetCompaniesAndQueueSizes(t *testing.T) {
	forwarderTest.SetEnvVars()
	arr, err := GetCompaniesAndQueueSizes()
	assert.NoError(t, err, "Dang it")
	for _, elem := range arr {
		fmt.Printf("Elem: %v\n", elem)
	}
}

func TestSetWarnedAt(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := SetWarnedAt(1)
	assert.NoError(t, err, "oh no!")
}

func TestDisableCompany(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := DisableCompany(1)
	assert.NoError(t, err, "oh no!")
}

func TestGetEndPointCfg(t *testing.T) {
	forwarderTest.SetEnvVars()
	v, err := GetEndPointCfg(1)
	assert.NoError(t, err)
	fmt.Printf("%#v\n", v)
}

func TestWriteQueueCheckpoint(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := WriteQueueCheckpoint(1, 1, 2, 3, 4, 5)
	assert.NoError(t, err)
}

func TestPurgeOldRows(t *testing.T) {
	forwarderTest.SetEnvVars()
	err := PurgeOldRows()
	assert.NoError(t, err)
}
