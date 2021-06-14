package forwarderDb

import (
	"fmt"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
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
	forwarderTest.SetEnvVars()

	forwarderStats.AddForwardedAtH(1)
	forwarderStats.AddErrorMessage(1,"klaskatt2")
	forwarderStats.AddExample(1, "some example")
	forwarderStats.AddLost(1)
	forwarderStats.AddLost(1)
	forwarderStats.AddLost(1)
	forwarderStats.AddTimeout(1)
	forwarderStats.AddTimeout(1)
	WriteStatsToDb()
}