package individual_queues

import (
	"encoding/json"
	"fmt"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	"strconv"
	"sync"
)

var endpointIdToCfg = make(map[int]*forwarderDb.EndPointCfgStruct)

var touchedForwardId = make(map[int]bool)

var iqMutex sync.Mutex

func Cleanup() {
	endpointIdToCfg = make(map[int]*forwarderDb.EndPointCfgStruct)
	touchedForwardId = make(map[int]bool)
}

func TouchForwardId(forwardId int) error {
	if touchedForwardId[forwardId] {
		return nil
	}

	_, err := forwarderRedis.SetAdd("FWD_IQ_ACTIVE_ENDPOINTS_SET", forwardId)
	if nil != err {
		fmt.Printf("forwarder.IQ.TouchForwardId() error: %v\n", err)
		return err
	}

	return nil
}

func GetEndPointData(endPointId int) (*forwarderDb.EndPointCfgStruct, error) {
	iqMutex.Lock()
	defer iqMutex.Unlock()

	if val, ok := endpointIdToCfg[endPointId]; ok {
		return val, nil
	}

	fmt.Printf("forwarder.IQ.GetEndPointData(): Local cache miss, trying redis. id=%d\n", endPointId)

	// ok try Redis
	key := "FWD_IQ_GETENDPOINTDATA_" + strconv.Itoa(endPointId)
	byteArray, _ := forwarderRedis.Get(key)
	if nil != byteArray {
		var cfg forwarderDb.EndPointCfgStruct
		if err := json.Unmarshal(byteArray, &cfg); err != nil {
			return nil, err
		}
		endpointIdToCfg[endPointId] = &cfg
		return &cfg, nil
	}

	fmt.Printf("forwarder.IQ.GetEndPointData(): Redis miss, trying db. id=%d\n", endPointId)

	// wasn't in redis. Load from db.
	cfg, err := forwarderDb.GetEndPointCfg(endPointId)
	if nil != err {
		endpointIdToCfg[endPointId] = nil
		return nil, err
	}

	if cfg == nil {
		endpointIdToCfg[endPointId] = nil
		return nil, fmt.Errorf("forwarder.individual_queues.GetEndPointData() db returned nil cfg: %v", err)
	}

	endpointIdToCfg[endPointId] = cfg

	b, err := json.Marshal(cfg)
	if nil != err {
		return cfg, err
	}

	err = forwarderRedis.Set(key, b)
	if err != nil {
		return cfg, fmt.Errorf("forwarder.individual_queues.GetEndPointData() redis failed to cache cfg: %v", err)
	}

	_, err = forwarderRedis.Expire(key, 10 * 60)
	if nil != err {
		fmt.Printf("forwarder.IQ.GetEndPointData(): Failed to set TTL for %s: %v", key, err)
	}

	return cfg, nil
}

// ReCalculateUsersQueueSizes is (should be) called after a break. So we can assume we can set currently processing to 0
func ReCalculateUsersQueueSizes(projectID string, endPointIdToSubsId map[int]string) error {

	var subscriptionIds []string
	for _, subsId := range endPointIdToSubsId {
		subscriptionIds = append(subscriptionIds, subsId)
	}

	sizeMap, err := forwarderPubsub.CheckNbrItemsPubsubs(projectID, subscriptionIds)
	if nil != err {
		return fmt.Errorf("forwarder.individual_queues.ReCalculateUsersQueueSizes() Failed to check queue sizes: %v", err)
	}

	forwarderRedis.Del("FWD_IQ_ACTIVE_ENDPOINTS_SET")
	for endPointId, subsName := range endPointIdToSubsId {

		if val, ok := sizeMap[subsName]; ok {
			forwarderRedis.SetInt64("FWD_IQ_QS_" + strconv.Itoa(endPointId), val)
			forwarderRedis.SetAdd("FWD_IQ_ACTIVE_ENDPOINTS_SET", endPointId)
		} else {
			forwarderRedis.SetInt64("FWD_IQ_QS_" + strconv.Itoa(endPointId), int64(0))
		}

		// Assume nothing is currently processing. This method is only called after a certain time of Pause.
		forwarderRedis.SetInt64("FWD_IQ_PS_" + strconv.Itoa(endPointId), int64(0))
	}

	return nil
}
