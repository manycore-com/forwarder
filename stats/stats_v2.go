package stats

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

type Stats struct {
	ReceivedAtH    [24]int  // One per item put on responder
	EnterQueueAtH  [24]int  // One per message we put on forward 1. >= ReceivedAtH
	AgeWhenForward [72]int  // there are unused gaps. Offsets used are in TruncatedHourList
	ForwardedAtH   [24]int
	Example        string
	ErrorMessage   string
	NbrLost        int
	NbrTimeout     int // mutual exclusive with NbrLost
	CompanyId      int // need this as map key is endpoint id
}

// StatsMap 2021-07-12 is now endpont->struct, not company->struct
var StatsMap = make(map[int]*Stats)

var statsMutex sync.Mutex

func CleanupV2() {
	fmt.Printf("forwarder.stats.CleanupV2(): wiping data\n")
	StatsMap = make(map[int]*Stats)
}

func epochThenToOffs(epochThen int64) int {
	var now = time.Now().Unix()
	if now < epochThen {
		fmt.Printf("forwarder.stats.epochThenToOffs() provided timestamp is in the future: %d\n", epochThen)
		return 0
	}

	var offset = int((now - epochThen) / 3600)
	return offset
}

func touchElem(companyId int, endpointId int) *Stats {

	theElem, elementExists := StatsMap[endpointId]
	if ! elementExists {
		theElem = &Stats{}
		theElem.CompanyId = companyId
		StatsMap[endpointId] = theElem
	}

	return theElem
}

func AddReceivedAtH(companyId int, endpointId int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	hour := time.Now().UTC().Hour()
	theElem := touchElem(companyId, endpointId)
	theElem.ReceivedAtH[hour] += 1
	return theElem.ReceivedAtH[hour]
}

func AddEnterQueueAtH(companyId int, endpointId int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	hour := time.Now().UTC().Hour()
	theElem := touchElem(companyId, endpointId)
	theElem.EnterQueueAtH[hour] += 1
	return theElem.EnterQueueAtH[hour]
}

func AddAgeWhenForward(companyId int, endpointId int, ts int64) int {
	offs := TsToTruncOffs(ts)

	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId, endpointId)
	theElem.AgeWhenForward[offs] += 1
	return theElem.AgeWhenForward[offs]
}

func AddForwardedAtH(companyId int, endpointId int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	hour := time.Now().UTC().Hour()
	theElem := touchElem(companyId, endpointId)
	theElem.ForwardedAtH[hour] += 1
	return theElem.ForwardedAtH[hour]
}

func AddExample(companyId int, endpointId int, example string) {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId, endpointId)
	theElem.Example = example
}

func AddErrorMessage(companyId int, endpointId int, errorMessage string) {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId, endpointId)
	theElem.ErrorMessage = errorMessage
}

func AddLost(companyId int, endpointId int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId, endpointId)
	theElem.NbrLost += 1
	return theElem.NbrLost
}

func AddTimeout(companyId int, endpointId int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId, endpointId)
	theElem.NbrTimeout += 1
	return theElem.NbrTimeout
}

func GetMemUsageMb() (uint64, uint64, uint64, uint32) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	return m.Alloc / 1024 / 1024, m.TotalAlloc / 1024 / 1024, m.Sys / 1024 / 1024, m.NumGC
}

func GetMemUsageStr() string {
	alloc, totalAlloc, sys, numgc := GetMemUsageMb()
	return fmt.Sprintf("Alloc: %vMB, total alloc: %vMB, sys: %v, # gc: %v", alloc, totalAlloc, sys, numgc)
}

var TruncatedHourList = [24]int {
	 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 16, 20, 24, 28, 32, 36, 42, 48, 54, 60, 66,
}

var OffsToTruncatedHour = [72]int {
	 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 12, 12, 12, 16, 16, 16, 16, 20, 20, 20, 20,
	24, 24, 24, 24, 28, 28, 28, 28, 32, 32, 32, 32, 36, 36, 36, 36, 36, 36, 42, 42, 42, 42, 42, 42,
	48, 48, 48, 48, 48, 48, 54, 54, 54, 54, 54, 54, 60, 60, 60, 60, 60, 60, 66, 66, 66, 66, 66, 66,
}

func TsToTruncOffs(ts int64) int {
	var now = time.Now().Unix()
	offs := (now - ts) / int64(3600)

	if offs < 0 {
		fmt.Printf("offs < 0: %v\n", offs)
		return 0
	} else if offs > 71 {
		return 66
	} else {
		return OffsToTruncatedHour[offs]
	}
}
