package forwarderDb

import (
	"fmt"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidateUrlPath(t *testing.T) {

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

	err := UpdateLastInMessage(1, "klaskatt3", 30, 11)
	if nil != err {
		t.Errorf("failed to update: %v", err)
	}

}