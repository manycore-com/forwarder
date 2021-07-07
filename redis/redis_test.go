package redis

import (
	"fmt"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var DISPENSIBLE_KEY = "somerandomkey8764827642387462"

func TestGet(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	ba, err := Get("thiskeydoesntexist")
	fmt.Printf("not found:  %v %v\n", ba, err)
}

func TestSadd(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = SetAdd(DISPENSIBLE_KEY, 1)
	assert.NoError(t, err)

	Del(DISPENSIBLE_KEY)
}

func TestSetMembers(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = SetAdd(DISPENSIBLE_KEY, 1)
	assert.NoError(t, err)
	_, err = SetAdd(DISPENSIBLE_KEY, 2)
	assert.NoError(t, err)

	xx, err := SetMembers(DISPENSIBLE_KEY)
	assert.NoError(t, err)
	assert.Len(t, xx, 2)

	Del(DISPENSIBLE_KEY)
}

func TestSetMembersInt(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = SetAdd(DISPENSIBLE_KEY, 1)
	assert.NoError(t, err)
	_, err = SetAdd(DISPENSIBLE_KEY, 223456)
	assert.NoError(t, err)

	xx, err := SetMembersInt(DISPENSIBLE_KEY)
	assert.NoError(t, err)
	assert.Len(t, xx, 2)
	assert.Equal(t, 1, xx[0])
	assert.Equal(t, 223456, xx[1])

	Del(DISPENSIBLE_KEY)
}

func TestListLRangeInt(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = ListRPush(DISPENSIBLE_KEY, 7)
	assert.NoError(t, err)
	_, err = ListRPush(DISPENSIBLE_KEY, 8)
	assert.NoError(t, err)
	_, err = ListRPush(DISPENSIBLE_KEY, 9)
	assert.NoError(t, err)

	xx, err := ListLRangeInt(DISPENSIBLE_KEY, 0, 1000000)
	assert.Len(t, xx, 3)
	assert.Equal(t, 7, xx[0])
	assert.Equal(t, 8, xx[1])
	assert.Equal(t, 9, xx[2])

	Del(DISPENSIBLE_KEY)
}

func TestSetInt64(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	Del(DISPENSIBLE_KEY)

	err = SetInt64(DISPENSIBLE_KEY, int64(123))
	assert.NoError(t, err, "Error")

	x, err := Inc(DISPENSIBLE_KEY)

	assert.NoError(t, err, "Errors")
	assert.Equal(t, 124, x)

	Del(DISPENSIBLE_KEY)
}

func TestExpire(t *testing.T) {
	forwarderTest.SetEnvVars()

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	Del(DISPENSIBLE_KEY)
	Inc(DISPENSIBLE_KEY)
	x, err := Expire(DISPENSIBLE_KEY, 2)
	assert.NoError(t, err, "Error")
	assert.Equal(t, 1, x)
	x, err = Inc(DISPENSIBLE_KEY)
	assert.NoError(t, err, "Error")
	assert.Equal(t, 2, x)

	time.Sleep(time.Second * 3)
	x, err = GetInt(DISPENSIBLE_KEY)
	assert.NoError(t, err, "Error")
	assert.Equal(t, 0, x)
}