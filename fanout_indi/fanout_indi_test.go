package fanout_indi

import (
	forwarderKafka "github.com/manycore-com/forwarder/kafka"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestKafka(t *testing.T) {
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

	err := forwarderKafka.Env()
	assert.NoError(t, err)
	defer forwarderKafka.Cleanup()

	elem := forwarderPubsub.PubSubElement{
		CompanyID: 1,
		ESPJsonString: `[{"attempt": "49", "category": ["emailer_campaign_5e39f2bd0a2aa200f4660124", "emailer_touch_5e39f2bd0a2aa200f4660129"], "email": "royandroger@petrolstation.net", "emailer_message_id": "5e409bbc6a536f00014f498d", "event": "deferred", "ip": "168.245.76.49", "response": "550 5.7.1 [C12] RBL Restriction: - <168.245.76.49> - See http://csi.cloudmark.com/reset-request/?ip=168.245.76.49", "sg_event_id": "ZGVmZXJyZWQtNDktMTQ1OTAxNzItVS11ZW1Qd2FRWmlUdE9XVGdkZUFQZy0w", "sg_message_id": "U-uemPwaQZiTtOWTgdeAPg.filterdrecv-p3mdw1-77d77b4d7d-b9lcr-18-5E40ABD4-60.0", "smtp-id": "<U-uemPwaQZiTtOWTgdeAPg@ismtpd0047p1mdw1.sendgrid.net>", "timestamp": 1621963262, "tls": 0}]`,
	}

	addKafkaMessage(&elem)
	err = flushKafkaMessages()
	assert.NoError(t, err)
}
