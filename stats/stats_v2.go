package stats

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

type StatsV2 struct {
	ReceivedAtH   	[24]int
	AgeWhenForward	[24]int
	ForwardedAtH    [24]int
	Example         string
	ErrorMessage    string
	NbrLostMessages int
}

var StatsMapV2 = make(map[int]*StatsV2)

var statsMutexV2 sync.Mutex

func CleanupV2() {
	fmt.Printf("forwarder.stats.CleanupV2(): wiping data\n")
	StatsMapV2 = make(map[int]*StatsV2)
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

func touchElemV2(companyId int) *StatsV2 {

	theElem, elementExists := StatsMapV2[companyId]
	if ! elementExists {
		theElem = &StatsV2{}
		StatsMapV2[companyId] = theElem
	}

	return theElem
}

func AddReceivedAtHV2(companyId int) int {
	statsMutexV2.Lock()
	defer statsMutexV2.Unlock()

	hour := time.Now().UTC().Hour()
	theElem := touchElemV2(companyId)
	theElem.ReceivedAtH[hour] += 1
	return theElem.ReceivedAtH[hour]
}

func AddAgeWhenForwardV2(companyId int, ts int64) int {
	offs := epochThenToOffs(ts)
	if offs > 23 {
		fmt.Printf("forwarder.stats.AddAgeWhenForward(): companyId:%d message %d old!\n", companyId, offs)
		return -1
	}

	statsMutexV2.Lock()
	defer statsMutexV2.Unlock()

	theElem := touchElemV2(companyId)
	theElem.AgeWhenForward[offs] += 1
	return theElem.AgeWhenForward[offs]
}

func AddForwardedAtHV2(companyId int) int {
	statsMutexV2.Lock()
	defer statsMutexV2.Unlock()

	hour := time.Now().UTC().Hour()
	theElem := touchElemV2(companyId)
	theElem.ForwardedAtH[hour] += 1
	return theElem.ForwardedAtH[hour]
}

func AddExampleV2(companyId int, example string) {
	statsMutexV2.Lock()
	defer statsMutexV2.Unlock()

	theElem := touchElemV2(companyId)
	theElem.Example = example
}

func AddErrorMessageV2(companyId int, errorMessage string) {
	statsMutexV2.Lock()
	defer statsMutexV2.Unlock()

	theElem := touchElemV2(companyId)
	theElem.ErrorMessage = errorMessage
}

func AddLostMessagesV2(companyId int) int {
	statsMutexV2.Lock()
	defer statsMutexV2.Unlock()

	theElem := touchElemV2(companyId)
	theElem.NbrLostMessages += 1
	return theElem.NbrLostMessages
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
