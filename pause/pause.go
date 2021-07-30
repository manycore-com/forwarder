package pause

import (
	"context"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderRedis "github.com/manycore-com/forwarder/redis"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"google.golang.org/api/cloudscheduler/v1"
	"os"
	"strconv"
)

var projectId = ""
var nbrHash = -1
var gcpLocation = ""
func Env() error {
	projectId = os.Getenv("PROJECT_ID")

	var err error

	if "" == os.Getenv("NBR_HASH") {
		return fmt.Errorf("forwarder.pause.Env() Missing NBR_HASH environment variable")
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
		return fmt.Errorf("forwarder.pause.Env() You need to set GCP_LOCATION")
	} else {
		gcpLocation = os.Getenv("GCP_LOCATION")
	}

	return nil
}

// Pause crates one pause row per hash, and it pauses Google Cloud Scheduler
func Pause(jobNames []string) error {
	err := Env()
	if nil != err {
		return err
	}

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.pause.Pause(): v%s Failed to init Redis: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer forwarderRedis.Cleanup()

	err = forwarderRedis.SetInt64("IN_PAUSE", int64(1))
	if nil != err {
		return err
	}

	_, err = forwarderRedis.Expire("IN_PAUSE", 60 * 60 * 6)
	if nil != err {
		forwarderRedis.Del("IN_PAUSE")
	}

	err = forwarderDb.CreatePauseRows(nbrHash)
	if nil != err {
		forwarderDb.DeleteAllPauseRows()
		forwarderRedis.Del("IN_PAUSE")
		return err
	}

	ctx := context.Background()
	cloudschedulerService, err := cloudscheduler.NewService(ctx)
	if err != nil {
		forwarderDb.DeleteAllPauseRows()
		forwarderRedis.Del("IN_PAUSE")
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
			forwarderRedis.Del("IN_PAUSE")
			return err
		}

		fmt.Printf("%#v\n", resp)
	}

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.pause.Pause() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return nil
}

func Resume(jobNames []string) error {
	err := Env()
	if nil != err {
		return err
	}

	err = forwarderRedis.Init()
	if nil != err {
		fmt.Printf("forwarder.pause.Resume(): v%s Failed to init Redis: %v\n", forwarderCommon.PackageVersion, err)
		return err
	}

	defer forwarderRedis.Cleanup()

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

	err = forwarderRedis.Del("IN_PAUSE")
	if nil != err {
		return fmt.Errorf("forwarder.pause.Resume() Failed to delete redis entry: %v\n", err)
	}

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.pause.Resume() ok. v%s, Memstats: %s\n", forwarderCommon.PackageVersion, memUsage)

	return nil
}

