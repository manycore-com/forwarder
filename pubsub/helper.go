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

// CheckNbrItemsPubsub Note: The number shows by delay 0..60s
func CheckNbrItemsPubsub(projectID string, subscriptionId string) (int64,error) {
	ctx := context.Background()

	// Creates a client.
	client, err := monitoring.NewMetricClient(ctx)  // MetricServiceClient in Py
	if err != nil {
		return 0,err
	}
	defer client.Close()

	startTime := time.Now().UTC().Add(time.Second * -120)
	endTime := time.Now().UTC()
	var req *monitoringpb.ListTimeSeriesRequest = &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/" + projectID,
		Filter: `metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" resource.labels.subscription_id="` + subscriptionId + `"`,
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

	var resultingNumber int64 = -1
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return 0, fmt.Errorf("could not read time series value: %v", err)
		}

		resultingNumber = resp.GetPoints()[len (resp.GetPoints()) - 1].Value.GetInt64Value()

		break
	}

	if resultingNumber >= 0 {
		return resultingNumber, nil
	}

	return 0,fmt.Errorf("Failed to find time series data for %s", subscriptionId)
}

func SetupClientAndTopic(projectID string, topicId string) (*context.Context, *pubsub.Client, * pubsub.Topic, error) {
	if "" == topicId {
		return nil, nil, nil, fmt.Errorf("forwarder.pubsub.setupClientAndTopic(): Critical Error: Missing Topic ID")
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("forwarder.pubsub.setupClientAndTopic(): Critical Error: Failed to instantiate Client: %v", err)
	}

	var topicInstance = client.Topic(topicId)

	return &ctx, client, topicInstance, nil
}

func PushElemToPubsub(ctx *context.Context, topic *pubsub.Topic, elem *PubSubElement) error {
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
		return fmt.Errorf("forwarder.pubsub.PushElemToPubsub(): Error: Failed to get result: %v", waitErr)
	}

	return nil
}

func PushJsonStringToPubsub(ctx *context.Context, topic *pubsub.Topic, jsonString string) error {
	nextForwardPublishResult := topic.Publish(*ctx, &pubsub.Message{
		Data: []byte(jsonString),
	})

	// id, waiterr
	_, waitErr := nextForwardPublishResult.Get(*ctx)
	if waitErr != nil {
		return fmt.Errorf("forwarder.pubsub.PushJsonStringToPubsub(): Error: Failed to get result: %v", waitErr)
	}

	return nil
}
