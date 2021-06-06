package forwarderDb

import (
	"fmt"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpdateUsage(t *testing.T) {
	forwarderTest.SetEnvVars()

	_, _, _, err := UpdateUsage(1,  1, 2, 3, 4, 5, "Err msg", 6)
	assert.NoError(t, err, "Failed to update usage")
}

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

func TestUpdateErrorMessage(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := UpdateLastInMessage(1, "klaskatt3 90211", 30, 11)
	if nil != err {
		t.Errorf("failed to update: %v", err)
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
	forwarderTest.SetEnvVars()

	forwarderStats.AddForwardedAtHV2(1)
	forwarderStats.AddErrorMessageV2(1,"klaskatt2")
	forwarderStats.AddExampleV2(1, "some example")
	WriteStatsToDbV2()
}