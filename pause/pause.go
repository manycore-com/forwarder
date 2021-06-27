package pause

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"google.golang.org/api/cloudscheduler/v1"
	"os"
	"strconv"
	"sync"
	"time"
)

var projectId = ""
var nbrHash = -1
var gcpLocation = ""
func env() error {
	projectId = os.Getenv("PROJECT_ID")

	var err error

	if "" == os.Getenv("NBR_HASH") {
		return fmt.Errorf("forwarder.pause.env() Missing NBR_HASH environment variable")
	} else {
		nbrHash, err = strconv.Atoi(os.Getenv("NBR_HASH"))
		if nil != err {
			return fmt.Errorf("failed to parse integer NBR_HASH: %v", err)
		}

		if 1 > nbrHash {
			return fmt.Errorf("mandatory NBR_HASH environent variable must be at least 1: %v", nbrHash)
		}

		if 1024 < nbrHash {
			return fmt.Errorf("optional NBR_HASH environent should be at most 1024: %v", nbrHash)
		}
	}

	if "" == os.Getenv("GCP_LOCATION") {
		return fmt.Errorf("forwarder.pause.env() You need to set GCP_LOCATION")
	} else {
		gcpLocation = os.Getenv("GCP_LOCATION")
	}

	return nil
}

// Pause crates one pause row per hash, and it pauses Google Cloud Scheduler
func Pause(jobNames []string) error {
	err := env()
	if nil != err {
		return err
	}

	err = forwarderDb.CreatePauseRows(nbrHash)
	if nil != err {
		forwarderDb.DeleteAllPauseRows()
		return err
	}

	ctx := context.Background()
	cloudschedulerService, err := cloudscheduler.NewService(ctx)
	if err != nil {
		forwarderDb.DeleteAllPauseRows()
		return err
	}

	for _, jobName := range jobNames {
		fmt.Printf("Jobname: %s\n", jobName)
		name := "projects/" + projectId + "/locations/" + gcpLocation + "/jobs/" + jobName
		rb := &cloudscheduler.PauseJobRequest{
			// TODO: Add desired fields of the request body.          ??? Meng has no idea
		}

		resp, err := cloudschedulerService.Projects.Locations.Jobs.Pause(name, rb).Context(ctx).Do()
		if err != nil {
			forwarderDb.DeleteAllPauseRows()
			return err
		}

		fmt.Printf("%#v\n", resp)
	}

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.pause.Pause() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return nil
}

func Resume(jobNames []string) error {
	err := env()
	if nil != err {
		return err
	}

	ctx := context.Background()

	cloudschedulerService, err := cloudscheduler.NewService(ctx)
	if err != nil {
		return fmt.Errorf("forwarder.pause.Resume() NewService failed: %v\n", err)
	}

	for _, jobName := range jobNames {
		fmt.Printf("Jobname: %s\n", jobName)
		name := "projects/" + projectId + "/locations/" + gcpLocation + "/jobs/" + jobName
		rb := &cloudscheduler.ResumeJobRequest{
			// TODO: Add desired fields of the request body.          ??? Meng has no idea
		}

		resp, err := cloudschedulerService.Projects.Locations.Jobs.Resume(name, rb).Context(ctx).Do()
		if err != nil {
			forwarderDb.DeleteAllPauseRows()
			return fmt.Errorf("forwarder.pause.Resume() Jobs.Resume failed: %v\n", err)
		}

		fmt.Printf("%#v\n", resp)
	}

	err = forwarderDb.DeleteAllPauseRows()
	if nil != err {
		return fmt.Errorf("forwarder.pause.Resume() Failed to delete pause rows: %v\n", err)
	}

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.pause.Resume() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return nil
}

func WriteBackMessages(nbrWriteBackWorkers int, writeBackChan *chan *forwarderPubsub.PubSubElement, writeBackWaitGroup *sync.WaitGroup, topicName string) {
	for i:=0; i<nbrWriteBackWorkers; i++ {
		writeBackWaitGroup.Add(1)

		go func(writeBackChan *chan *forwarderPubsub.PubSubElement, writeBackWaitGroup *sync.WaitGroup, topicName string) {
			defer writeBackWaitGroup.Done()

			ctx1, client, topic, err := forwarderPubsub.SetupClientAndTopic(projectId, topicName)
			if err != nil {
				fmt.Printf("forwarder.pause.writeBackMessages(%s): Critical Error: Failed to instantiate Client: %v\n", topicName, err)
				return
			}

			if nil != client {
				defer client.Close()
			}

			for {
				elem := <- *writeBackChan
				if nil == elem {
					fmt.Printf("forwarder.pause.writeBackMessages(%s): Go routine done\n", topicName)
					return
				}

				err = forwarderPubsub.PushElemToPubsub(ctx1, topic, elem)
				if nil != err {
					fmt.Printf("forwarder.pause.writeBackMessages(%s): error pushing: %v\n", topicName, err)
				} else {
					fmt.Printf("forwarded 1\n")
				}
			}

		} (writeBackChan, writeBackWaitGroup, topicName)
	}
}

var CompanyCountMap map[int]int

func MoveAndCount(subscriptionPairs [][]string, reverse bool) bool {
	CompanyCountMap = make(map[int]int)

	var nbrWriteBackWorkers int = 16
	var seriousError bool = false
	var waitGroup sync.WaitGroup

	for _, subscriptionPair := range subscriptionPairs {
		var sourceSubscription string
		var destSubscription string
		if reverse {
			sourceSubscription = subscriptionPair[1]
			destSubscription = subscriptionPair[0]
		} else {
			sourceSubscription = subscriptionPair[0]
			destSubscription = subscriptionPair[1]
		}

		waitGroup.Add(1)

		go func(waitGroup *sync.WaitGroup, sourceSubscription string, destSubscription string) {
			defer waitGroup.Done()

			// Setup writeback queue
			writeBackChan := make(chan *forwarderPubsub.PubSubElement, nbrWriteBackWorkers)
			defer close(writeBackChan)
			var writeBackWaitGroup sync.WaitGroup

			// Starts async writers
			WriteBackMessages(nbrWriteBackWorkers, &writeBackChan, &writeBackWaitGroup, destSubscription)
			defer func(nbrWorkers int, writeBackWaitGroup *sync.WaitGroup) {
				for i:=0; i<nbrWorkers; i++ {
					writeBackChan <- nil
				}

				writeBackWaitGroup.Wait()
			} (nbrWriteBackWorkers, &writeBackWaitGroup)

			ctx := context.Background()
			client, clientErr := pubsub.NewClient(ctx, projectId)
			if clientErr != nil {
				fmt.Printf("forwarder.pause.MoveAndCount(%s) failed to create client: %v\n", sourceSubscription, clientErr)
				seriousError = true
				return
			}

			if nil != client {
				defer client.Close()
			}

			subscription := client.Subscription(sourceSubscription)
			subscription.ReceiveSettings.Synchronous = true
			subscription.ReceiveSettings.MaxOutstandingMessages = nbrWriteBackWorkers
			subscription.ReceiveSettings.MaxOutstandingBytes = 11000000
			fmt.Printf("Source subscription: %v\n", sourceSubscription)

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(500) * time.Second)
			defer cancel()

			var mu sync.Mutex
			var runTick = true
			var startMs = time.Now().UnixNano() / 1000000
			var lastAtMs = startMs + 5000 // 3000ms was too low for local machine. 8000ms was enough. It's reset after first received message.
			go func() {
				for {
					time.Sleep(time.Millisecond * 100)
					mu.Lock()
					var copyOfRunTick = runTick
					var copyOfLastAtMs = lastAtMs
					mu.Unlock()

					if !copyOfRunTick {
						return
					}

					var rightNow = time.Now().UnixNano() / 1000000
					if (int64(6000) + copyOfLastAtMs) < rightNow {
						fmt.Printf("forwarder.pause.MoveAndCount.func(%s) Killing Receive due to %dms inactivity.\n", sourceSubscription, 6000)
						mu.Lock()
						runTick = false
						mu.Unlock()
						cancel()
						return
					}
				}
			} ()

			fmt.Printf("Before Receive\n")
			err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				fmt.Printf("Enter Receive\n")
				mu.Lock()
				var runTickCopy = runTick
				mu.Unlock()

				if ! runTickCopy {
					fmt.Printf("forwarder.pause.MoveAndCount.Receive(%s) we are canceled.\n", sourceSubscription)
					defer msg.Nack()
					return
				}

				var elem forwarderPubsub.PubSubElement
				err := json.Unmarshal(msg.Data, &elem)

				if nil == err {
					mu.Lock()

					// Is there an item?
					if val, ok := CompanyCountMap[elem.CompanyID]; ok {
						CompanyCountMap[elem.CompanyID] = val + 1
					} else {
						CompanyCountMap[elem.CompanyID] = 1
					}

					lastAtMs = time.Now().UnixNano() / 1000000

					mu.Unlock()

					fmt.Printf("forwarder.pause.MoveAndCount.Receive(%s): counted message %v\n", sourceSubscription, elem)
					writeBackChan <- &elem
					msg.Ack()

				} else {
					fmt.Printf("forwarder.pause.MoveAndCount.Receive(%s): Error: failed to Unmarshal: %v\n", sourceSubscription, err)
					msg.Ack()  // Valid or not, Ack to get rid of it
				}
			})

			if nil != err {
				fmt.Printf("forwarder.pause.MoveAndCount(%s) Received failed: %v\n", sourceSubscription, err)
				// The defer anonymous function will write -1 instead
				seriousError = true
			}

		} (&waitGroup, sourceSubscription, destSubscription)
	}

	waitGroup.Wait()

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.pause.MoveAndCount() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return seriousError
}

func CountAndCheckpoint2(subscriptionPairs [][]string, jobNames []string) error {
	env()

	// true -> serious error
	if MoveAndCount(subscriptionPairs, false) {
		return fmt.Errorf("forwarder.pause.CountAndCheckpoint2(): Serious error trying to check queue size")
	}

	// true -> serious error
	if MoveAndCount(subscriptionPairs, true) {
		return fmt.Errorf("forwarder.pause.CountAndCheckpoint2(): Serious error trying to check queue size (2)")
	}

	var mapOfProcessed = make(map[int]bool)

	// Now we know that CompanyCountMap is initialized correctly
	for companyId, queueSize := range CompanyCountMap {
		mapOfProcessed[companyId] = true
		err := forwarderDb.WriteQueueCheckpoint(companyId, queueSize)
		if nil != err {
			fmt.Printf("forwarder.pause.CountAndCheckpoint2(): Failed to update cid:%d err:%v\n", companyId, err)
			return err
		}
	}

	// We want to take the companies that were active lately but had no items in the queue now, and write an empty
	// checkpoint for them
	companies, err := forwarderDb.GetLatestActiveCompanies()
	if nil == err {
		for _, companyId := range companies {
			fmt.Printf("forwarder.pause.CountAndCheckpoint2() Writing active companies with nothing on resend queue: %d\n", companyId)
			if ! mapOfProcessed[companyId] {
				err := forwarderDb.WriteQueueCheckpoint(companyId, 0)
				if nil != err {
					fmt.Printf("forwarder.pause.CountAndCheckpoint2(): Failed to update (2) cid:%d err:%v\n", companyId, err)
					return err
				}

			}
		}
	} else {
		fmt.Printf("forwarder.pause.CountAndCheckpoint2() GetLatestActiveCompanies failed: %v\n", err)
	}

	err = Resume(jobNames)

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.pause.CountAndCheckpoint2() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return err
}
