package stats

import (
	"fmt"
	"runtime"
	"sync"
)

type Stats struct {
	PollOk       int
	ForwardError int
	ForwardOk    int
	ForwardDrop  int
	ErrorMessage string
	InMessage    string
}

// StatsMap should only be used directly if all worker threads are done.
var StatsMap = make(map[int]*Stats)

var statsMutex sync.Mutex

func Cleanup() {
	StatsMap = make(map[int]*Stats)
}

func touchElem(companyId int) *Stats {

	theElem, elementExists := StatsMap[companyId]
	if ! elementExists {
		theElem = &Stats{}
		StatsMap[companyId] = theElem
	}

	return theElem
}

func AddPollOk(companyId int, delta int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId)
	theElem.PollOk += delta
	return theElem.PollOk
}

func AddForwardError(companyId int, delta int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId)
	theElem.ForwardError += delta
	return theElem.ForwardError
}

func AddForwardOk(companyId int, delta int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId)
	theElem.ForwardOk += delta
	return theElem.ForwardOk
}

func AddForwardDrop(companyId int, delta int) int {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId)
	theElem.ForwardDrop += delta
	return theElem.ForwardDrop
}

func AddErrorMessage(companyId int, errorMessage string) {
	if "" == errorMessage {
		return
	}

	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId)
	theElem.ErrorMessage = errorMessage
}

func AddInMessage(companyId int, inMessage string) {
	if "" == inMessage {
		return
	}

	statsMutex.Lock()
	defer statsMutex.Unlock()

	theElem := touchElem(companyId)
	theElem.InMessage = inMessage
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