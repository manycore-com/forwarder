package queue_sizes

import (
	"fmt"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	"strconv"
	"time"
)

func CalculateCurrentQueueSize(endPointId int, resendQueueTemplates []string) (int, int, int, error) {

	var totalQueueSize = 0

	// Calculate size of resend queues
	for _, resendQueueTemplate := range resendQueueTemplates {
		for i := 0; i < 4; i++ {
			var name = "counting_" + fmt.Sprintf(resendQueueTemplate, i) + "_" + strconv.Itoa(endPointId)
			queueSize, err := forwarderRedis.GetInt(name)
			if nil != err {
				return -1, -1, -1, fmt.Errorf("forwarder.queue_sizes.CalculateCurrentQueueSize() failed reading from redis (1): %v", err)
			}

			totalQueueSize += queueSize
		}
	}

	// Get size of QS
	qsSize, err := forwarderRedis.GetInt("FWD_IQ_QS_" + strconv.Itoa(endPointId))
	if nil != err {
		return -1, -1, -1, fmt.Errorf("forwarder.queue_sizes.CalculateCurrentQueueSize() failed reading from redis (2): %v", err)
	}

	// Get size of PS
	psSize, err := forwarderRedis.GetInt("FWD_IQ_PS_" + strconv.Itoa(endPointId))
	if nil != err {
		return -1, -1, -1, fmt.Errorf("forwarder.queue_sizes.CalculateCurrentQueueSize() failed reading from redis (3): %v", err)
	}

	return totalQueueSize, qsSize, psSize, nil
}

func GetOldestAgeInResend(endPointId int, resendQueueTemplates []string) (int, error) {

	var now = time.Now().Unix()
	var lowestTs int64

	// Calculate size of resend queues
	for _, resendQueueTemplate := range resendQueueTemplates {
		for i := 0; i < 4; i++ {
			var name = "oldest_" + fmt.Sprintf(resendQueueTemplate, i) + "_" + strconv.Itoa(endPointId)
			ts, err := forwarderRedis.GetInt64(name)
			if nil != err {
				return -1, fmt.Errorf("forwarder.queue_sizes.GetOldestAgeInResend() failed reading from redis: %v", err)
			}

			if 0 != ts {
				if lowestTs == int64(0) || ts < lowestTs {
					lowestTs = ts
				}
			}
		}
	}

	if 0 == lowestTs {
		return 0, nil
	}

	return int(now - lowestTs), nil
}

func QueueCheckpoint(resendQueueTemplates []string) error {

	endpointCompanies, err := forwarderDb.GetLatestActiveEndpoints()
	if nil != err {
		return fmt.Errorf("forwarder.queue_sizes.QueueCheckpoint() failed to get active endpoints: %v", err)
	}

	for _, endpointCompany := range endpointCompanies {

		resendQueueSize, qsSize, psSize, err := CalculateCurrentQueueSize(endpointCompany.EndPointId, resendQueueTemplates)
		if nil != err {
			return err
		}

		maxAge, err := GetOldestAgeInResend(endpointCompany.EndPointId, resendQueueTemplates)
		if nil != err {
			return err
		}

		fmt.Printf("forwarder.queue_sizes.QueueCheckpoint() endPointId:%d resendQueueSize:%d qs:%d ps:%d maxAge:%d\n", endpointCompany.EndPointId, resendQueueSize, qsSize, psSize, maxAge)

		err = forwarderDb.WriteQueueCheckpoint(endpointCompany.EndPointId, endpointCompany.CompanyId, resendQueueSize, qsSize, psSize, maxAge)
		if nil != err {
			return fmt.Errorf("forwarder.queue_sizes.QueueCheckpoint() failed to save checkpoint to db: %v", err)
		}
	}

	return nil
}
