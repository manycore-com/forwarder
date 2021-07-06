package forwarderpubsub

import (
	"cloud.google.com/go/pubsub"
	"fmt"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"os"
	"testing"
)


func TestAgeInSecMessage(t *testing.T) {
	var msg pubsub.Message

	fmt.Printf("nsg %v\n", msg)
}


func TestXX(t *testing.T) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GOOGLE_APPLICATION_CREDENTIALS"))
	fmt.Printf("mem start: %s\n", forwarderStats.GetMemUsageStr())  // 1/1

	devprod := "dev"
	nextTopicId := "TESTING"

	const okPayload = `[
    {
      "email":"apa4@banan.com",
      "timestamp":1576683110,
      "smtp-id":"<14c5d75ce93.dfd.64b469@ismtpd-555>",
      "event":"delivered",
      "category":"cat facts",
      "marketing_campaign_id":"supercampaign",
      "sg_event_id":"sg_event_id",
      "sg_message_id":"sg_message_id"
    }
]`

	ctx1, client, nextForwardTopic, err := SetupClientAndTopic(os.Getenv("FORWARDER_TEST_PROJECT_ID"), nextTopicId)
	if err != nil {
		fmt.Printf("forwarder.forward.asyncFailureProcessing(%s): Critical Error: Failed to instantiate Client: %v\n", devprod, err)
		return
	}

	defer client.Close()

	for i:=0; i<30; i++ {

		err = PushJsonStringToPubsub(ctx1, nextForwardTopic, okPayload)
		if err != nil {
			fmt.Printf("forwarder.forward.asyncFailureProcessing(%s): Error: Failed to send to %s pubsub: %v\n", devprod, nextTopicId, err)
			continue
		}

		fmt.Printf("forwarder.forward.asyncFailureProcessing(%s): Success. Forwarded topic=%s \n", devprod, nextTopicId)

	}

	fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())  // 4/27
}

// Nop:      Alloc: 1MB, total alloc: 1MB, sys: 66, # gc: 0      proj: TESTING
// Empty Q:  Alloc: 3MB, total alloc: 10MB, sys: 68, # gc: 4
// 1000 el:  Alloc: 2MB, total alloc: 10MB, sys: 68, # gc: 5
// 2000 el:  Alloc: 2MB, total alloc: 10MB, sys: 69, # gc: 5
// 3000 el:  Alloc: 3MB, total alloc: 10MB, sys: 68, # gc: 4
// 5000 el:  Alloc: 2MB, total alloc: 10MB, sys: 68, # gc: 5
func TestCheckNbrItemsPubsub(t *testing.T) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GOOGLE_APPLICATION_CREDENTIALS"))
	fmt.Printf("proj: %s\n", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	fmt.Printf("mem start: %s\n", forwarderStats.GetMemUsageStr())

	for i:=0; i<30; i++ {
		itemsPubsub, err := CheckNbrItemsPubsub(os.Getenv("FORWARDER_TEST_PROJECT_ID"), "TESTING")
		if err != nil {
			fmt.Printf("Error: %v\n",err)
			return
		}
		fmt.Printf("Items on q: %v\n", itemsPubsub)

		fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())
	}

	fmt.Printf("Done\n")
}

func TestCheckNbrItemsPubsubs(t *testing.T) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GOOGLE_APPLICATION_CREDENTIALS"))
	fmt.Printf("proj: %s\n", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	fmt.Printf("mem start: %s\n", forwarderStats.GetMemUsageStr())

	_, err := CheckNbrItemsPubsubs(os.Getenv("FORWARDER_TEST_PROJECT_ID"), []string{"TESTING"})
	if err != nil {
		fmt.Printf("Error: %v\n",err)
		return
	}
}

