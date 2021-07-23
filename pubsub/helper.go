package forwarderpubsub

import (
	monitoring "cloud.google.com/go/monitoring/apiv3"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/api/iterator"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
	"time"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func AgeInSecMessage(msg *pubsub.Message) float64 {
	return time.Now().UTC().Sub(msg.PublishTime).Seconds()
}

// CheckNbrItemsPubsubs Note: The number shows by delay 0..60s
func CheckNbrItemsPubsubs(projectID string, subscriptionIds []string) (map[string]int64,error) {
	if nil == subscriptionIds || 0 == len(subscriptionIds) {
		return nil, fmt.Errorf("forwarder.pubsub.CheckNbrItemsPubsubs() subscriptionIds array is empty")
	}

	ctx := context.Background()

	// Creates a client.
	client, err := monitoring.NewMetricClient(ctx)  // MetricServiceClient in Py
	if err != nil {
		return nil,err
	}
	defer client.Close()

	var subsToCount = make(map[string]int64)
	var subscriptionFilterString string
	for idx, item := range subscriptionIds {
		if 0 < idx {
			subscriptionFilterString += " OR "
		}

		subscriptionFilterString += `resource.labels.subscription_id="` + item + `"`

		subsToCount[item] = int64(0)
	}

	startTime := time.Now().UTC().Add(time.Second * -120)
	endTime := time.Now().UTC()
	var req *monitoringpb.ListTimeSeriesRequest = &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/" + projectID,
		Filter: `metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" AND (` + subscriptionFilterString + `)`,
		View: monitoringpb.ListTimeSeriesRequest_FULL,

		Interval: &monitoringpb.TimeInterval{
			StartTime: &timestamp.Timestamp{
				Seconds: startTime.Unix(),
			},
			EndTime: &timestamp.Timestamp{
				Seconds: endTime.Unix(),
			},
		},
	}

	it := client.ListTimeSeries(ctx, req)

	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("could not read time series value: %v", err)
		}

		var subscription = resp.GetResource().GetLabels()["subscription_id"]
		var resultingNumber = resp.GetPoints()[len (resp.GetPoints()) - 1].Value.GetInt64Value()
		subsToCount[subscription] = resultingNumber
	}

	return subsToCount, nil
}

// CheckNbrItemsPubsub Note: The number shows by delay 0..60s
func CheckNbrItemsPubsub(projectID string, subscriptionId string) (int64,error) {
	subsToCount, err := CheckNbrItemsPubsubs(projectID, []string{subscriptionId})
	if nil != err {
		return -1, err
	}

	return subsToCount[subscriptionId], nil
}

func SetupClient(projectID string) (*context.Context, *pubsub.Client, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, fmt.Errorf("forwarder.pubsub.setupClient(): Critical Error: Failed to instantiate Client: %v", err)
	}

	return &ctx, client, nil
}

func SetupClientAndTopic(projectID string, topicId string) (*context.Context, *pubsub.Client, *pubsub.Topic, error) {
	if "" == topicId {
		return nil, nil, nil, fmt.Errorf("forwarder.pubsub.setupClientAndTopic(): Critical Error: Missing Topic ID")
	}

	ctx, client, err := SetupClient(projectID)
	if err != nil {
		return nil, nil, nil, err
	}

	var topicInstance = client.Topic(topicId)

	return ctx, client, topicInstance, nil
}

func PushAndWaitElemToPubsub(ctx *context.Context, topic *pubsub.Topic, elem *PubSubElement) error {
	payload, err := json.Marshal(elem)
	if err != nil {
		return fmt.Errorf("asyncFailureProcessing(): Error: Failed to Marshal pubsub payload: %v", err)
	}

	nextForwardPublishResult := topic.Publish(*ctx, &pubsub.Message{
		Data: payload,
	})

	// id, waiterr
	_, waitErr := nextForwardPublishResult.Get(*ctx)
	if waitErr != nil {
		return fmt.Errorf("forwarder.pubsub.PushAndWaitElemToPubsub(): Error: Failed to get result: %v", waitErr)
	}

	return nil
}

func PushAndWaitJsonStringToPubsub(ctx *context.Context, topic *pubsub.Topic, jsonString string) error {
	nextForwardPublishResult := topic.Publish(*ctx, &pubsub.Message{
		Data: []byte(jsonString),
	})

	// id, waiterr
	_, waitErr := nextForwardPublishResult.Get(*ctx)
	if waitErr != nil {
		return fmt.Errorf("forwarder.pubsub.PushAndWaitJsonStringToPubsub(): Error: Failed to get result: %v", waitErr)
	}

	return nil
}
