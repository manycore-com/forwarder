package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"strconv"
)

var redisPool *redis.Pool

func Init() error {
	redisAddr := fmt.Sprintf("%s:%s",os.Getenv("REDISHOST"), os.Getenv("REDISPORT"))

	var maxConnections = 10
	if "" != os.Getenv("REDIS_POOL_MAX_CONN") {
		maxConnections, err := strconv.Atoi(os.Getenv("REDIS_POOL_MAX_CONN"))
		if nil != err {
			return fmt.Errorf("forwarder.redis.Init(): Failed to parse optional integer environment variable REDIS_POOL_MAX_CONN: %v\n", err)
		}

		if 1 > maxConnections {
			return fmt.Errorf("forwarder.redis.Init(): Optional is too low: %d, it needs to be 1.. ", maxConnections)
		}

		if 1000 < maxConnections {
			return fmt.Errorf("forwarder.redis.Init(): Optional is too high: %d, it needs to be <= 1000", maxConnections)
		}
	}

	redisPool = &redis.Pool{
		MaxIdle: maxConnections,
		Dial:    func() (redis.Conn, error) { return redis.Dial("tcp", redisAddr) },
	}

	return nil
}

func Cleanup() {
	if nil != redisPool {
		redisPool.Close()
		redisPool = nil
	}
}

func valueAsIntArray(interfaceArray []interface{}) ([]int, error) {
	var intArr []int
	for _, item := range interfaceArray {
		switch item.(type) {
		case []uint8:
			intVal, err := strconv.Atoi(string(item.([]byte)))
			if err != nil {
				return nil, fmt.Errorf("failed to convert to int: %v err=%v", item, err)
			}
			intArr = append(intArr, intVal)
		case int:
			intArr = append(intArr, item.(int))
		default:
			return nil, fmt.Errorf("failed to convert to int: val=%v type=%T", item, item)
		}
	}

	return intArr, nil
}

func Ping() error {
	conn := redisPool.Get()
	defer conn.Close()

	_, err := redis.String(conn.Do("PING"))
	if err != nil {
		return fmt.Errorf("cannot 'PING' redis: %v", err)
	}
	return nil
}

func Get(key string) ([]byte, error) {
	conn := redisPool.Get()
	defer conn.Close()

	var data []byte
	data, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return nil, fmt.Errorf("error getting key %s: %v", key, err)
	}

	return data, err
}

func Set(key string, value []byte) error {
	conn := redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}

		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}

	return nil
}

func Exists(key string) (bool, error) {
	conn := redisPool.Get()
	defer conn.Close()

	exists , err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return exists, fmt.Errorf("error checking if key %s exists: %v", key, err)
	}

	return exists, err
}

func Del(key string) error {
	conn := redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func Inc(key string) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("INCR", "visits"))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

// Set methods

// SetAdd adds item to set
func SetAdd(key string, val interface{}) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("SADD", key, val))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

func SetMembers(key string) ([]interface{}, error) {
	conn := redisPool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("SMEMBERS", key))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func SetMembersInt(key string) ([]int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("SMEMBERS", key))
	if err != nil {
		return nil, err
	}

	return valueAsIntArray(reply)
}

// List methods

func ListRPush(key string, val interface{}) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("RPUSH", key, val))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

func ListLRange(key string, startOffs int, stopOffs int) ([]interface{}, error) {
	conn := redisPool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("LRANGE", key, startOffs, stopOffs))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func ListLRangeInt(key string, startOffs int, stopOffs int) ([]int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("LRANGE", key, startOffs, stopOffs))
	if err != nil {
		return nil, err
	}

	return valueAsIntArray(reply)
}

