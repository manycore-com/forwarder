package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"strconv"
)

// If you use Redis on GCP, remember to setup a vpc connector
//   https://medium.com/google-cloud/using-memorystore-with-cloud-run-82e3d61df016

// Keys
//  individual_queues   FWD_IQ_GETENDPOINTDATA_#      Cached endpoint data for webhook_forwarder_poll_endpoint.id
//  individual_queues   FWD_IQ_QS_#                   Current size of webhook_forwarder_poll_endpoint.id #'s subscription
//  individual_queues   FWD_IQ_PS_#                   Number of events currently processing for webhook_forwarder_poll_endpoint.id
//  individual_queues   FWD_IQ_ACTIVE_ENDPOINTS_SET   This is a set of ids of active endpoints

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
		Dial:    func() (redis.Conn, error) {
			fmt.Printf("forwarder.redis.Init(): Connecting to %s\n", redisAddr)
			c, err:= redis.Dial("tcp", redisAddr)
			if err != nil {
				return nil, err
			}

			if "" != os.Getenv("REDISAUTH") {
				fmt.Printf("forwarder.redis.Init(): sending auth\n")
				if _, err := c.Do("AUTH", os.Getenv("REDISAUTH")); err != nil {
					c.Close()
					return nil, err
				}
			}

			fmt.Printf("forwarder.redis.Init(): connected\n")

			return c, nil
		},
	}

	return nil
}

func Cleanup() {
	if nil != redisPool {
		redisPool.Close()
		redisPool = nil
	}
}

func stringArrayAsInterfaceArray(arr []string) []interface{} {
	s := make([]interface{}, len(arr))
	for i, v := range arr {
		s[i] = v
	}
	return s
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

	getResult, err := conn.Do("GET", key)
	if nil != err {
		return nil, fmt.Errorf("forwarder.redis.Get() error getting key=%s: %v", key, err)
	}

	if getResult == nil {
		return nil, nil
	}

	data, err := redis.Bytes(getResult, err)
	if err != nil {
		return nil, fmt.Errorf("forwarder.redis.Get() error parse bytes key=%s: %v", key, err)
	}

	return data, err
}

func GetInt(key string) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	getResult, err := conn.Do("GET", key)
	if nil != err {
		return 0, fmt.Errorf("forwarder.redis.Get() error getting key=%s: %v", key, err)
	}

	if getResult == nil {
		return 0, nil
	}

	data, err := redis.Int(getResult, err)
	if err != nil {
		return 0, fmt.Errorf("forwarder.redis.Get() error parse bytes key=%s: %v", key, err)
	}

	return data, err
}

func MGetInt(keys []string) (map[string]int, error) {
	keysAsInterface := stringArrayAsInterfaceArray(keys)

	conn := redisPool.Get()
	defer conn.Close()

	mgetResult, err := conn.Do("MGET", keysAsInterface...)
	if nil != err {
		return nil, err
	}

	listSlice, ok := mgetResult.([]interface{})
	if ! ok {
		return nil, fmt.Errorf("forwarder.redis.MGetInt() result is not an interface array")
	}

	var result = make(map[string]int)
	for offs, key := range keys {
		var readval int

		if nil != listSlice[offs] {
			readval, _ = redis.Int(listSlice[offs], nil)
		}

		result[key] = readval
	}

	return result, nil
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

func Expire(key string, ttl int) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("EXPIRE", key, ttl))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

func SetInt64(key string, value int64) error {
	return Set(key, []byte(strconv.FormatInt(value, 10)))
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

func Incr(key string) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("INCR", key))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

func IncrBy(key string, by int) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("INCRBY", key, by))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

func Decr(key string) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("DECR", key))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

func DecrBy(key string, by int) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	counter, err := redis.Int(conn.Do("DECRBY", key, by))
	if err != nil {
		return -1, err
	}

	return counter, nil
}

// Set methods

// SetAddMember adds item to set
func SetAddMember(key string, val interface{}) (int, error) {
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

