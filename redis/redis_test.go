package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestX(t *testing.T) {

	redisAddr := fmt.Sprintf("%s:%s",os.Getenv("REDISHOST"), os.Getenv("REDISPORT"))

	const maxConnections = 10
	redisPool = &redis.Pool{
		MaxIdle: maxConnections,
		Dial:    func() (redis.Conn, error) { return redis.Dial("tcp", redisAddr) },
	}

	conn := redisPool.Get()
	defer conn.Close()


	counter, err := redis.Int(conn.Do("INCR", "visits"))
	if err != nil {
		fmt.Printf("Error incrementing visitor counter: %v\n", err)
		return
	}
	fmt.Printf("Visitor number: %d\n", counter)
}

func TestSadd(t *testing.T) {
	os.Setenv("REDISHOST", os.Getenv("FORWARDER_REDISHOST"))
	os.Setenv("REDISPORT", os.Getenv("FORWARDER_REDISPORT"))

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = SetAdd("somerandomkey8764827642387462", 1)
	assert.NoError(t, err)

	Del("somerandomkey8764827642387462")
}

func TestSetMembers(t *testing.T) {
	os.Setenv("REDISHOST", os.Getenv("FORWARDER_REDISHOST"))
	os.Setenv("REDISPORT", os.Getenv("FORWARDER_REDISPORT"))

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = SetAdd("somerandomkey8764827642387462", 1)
	assert.NoError(t, err)
	_, err = SetAdd("somerandomkey8764827642387462", 2)
	assert.NoError(t, err)

	xx, err := SetMembers("somerandomkey8764827642387462")
	assert.NoError(t, err)
	assert.Len(t, xx, 2)

	Del("somerandomkey8764827642387462")
}

func TestSetMembersInt(t *testing.T) {
	os.Setenv("REDISHOST", os.Getenv("FORWARDER_REDISHOST"))
	os.Setenv("REDISPORT", os.Getenv("FORWARDER_REDISPORT"))

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = SetAdd("somerandomkey8764827642387462", 1)
	assert.NoError(t, err)
	_, err = SetAdd("somerandomkey8764827642387462", 223456)
	assert.NoError(t, err)

	xx, err := SetMembersInt("somerandomkey8764827642387462")
	assert.NoError(t, err)
	assert.Len(t, xx, 2)
	assert.Equal(t, 1, xx[0])
	assert.Equal(t, 223456, xx[1])

	Del("somerandomkey8764827642387462")
}

func TestListLRangeInt(t *testing.T) {
	os.Setenv("REDISHOST", os.Getenv("FORWARDER_REDISHOST"))
	os.Setenv("REDISPORT", os.Getenv("FORWARDER_REDISPORT"))

	err := Init()
	assert.NoError(t, err)
	defer Cleanup()

	_, err = ListRPush("somerandomkey8764827642387462", 7)
	assert.NoError(t, err)
	_, err = ListRPush("somerandomkey8764827642387462", 8)
	assert.NoError(t, err)
	_, err = ListRPush("somerandomkey8764827642387462", 9)
	assert.NoError(t, err)

	xx, err := ListLRangeInt("somerandomkey8764827642387462", 0, 1000000)
	assert.Len(t, xx, 3)
	assert.Equal(t, 7, xx[0])
	assert.Equal(t, 8, xx[1])
	assert.Equal(t, 9, xx[2])

	Del("somerandomkey8764827642387462")
}
