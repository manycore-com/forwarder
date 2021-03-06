package esp

import (
	"bytes"
	"fmt"
	forwarderIQ "github.com/manycore-com/forwarder/individual_queues"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"io"
	"io/ioutil"
	"net/http"
)

func ForwardSg(devprod string, elem *forwarderPubsub.PubSubElement) (error, bool) {  // bool: any point to retry

	cfg, err := forwarderIQ.GetEndPointData(elem.EndPointId)
	if err != nil {
		return err, false
	}

	// ok, time to forward
	request, err := http.NewRequest("POST", cfg.ForwardEndpoint, bytes.NewReader([]byte(elem.ESPJsonString)))
	if err != nil {
		forwarderStats.AddErrorMessage(elem.CompanyID, elem.EndPointId, err.Error())
		return err, true
	}
	request.Close = true
	request.Header.Set("Content-Type", "application/json")

	// FIXME Decide if we make the verification or if we just defer it like this

	// For Sendgrid we know it's going to be X-Twilio-Email-Event-Webhook-Signature
	var header = GetSignHeaderName("sg")
	if "" != header && "" != elem.Sign {
		request.Header.Set(header, elem.Sign)
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		forwarderStats.AddErrorMessage(elem.CompanyID, elem.EndPointId, err.Error())
		if resp == nil {
			return err, false
		} else {
			return err, resp.StatusCode >= 500
		}
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			forwarderStats.AddErrorMessage(elem.CompanyID, elem.EndPointId, err.Error())
			fmt.Printf("forwarder.forward.forwardMg(%s): Error closing Body:%v\n", devprod, err)
		}
	} (resp.Body)

	fmt.Printf("forwarder.forward.forwardMg(%s): ok. Status:%v\n", devprod, resp.Status)

	_, err = ioutil.ReadAll(resp.Body)

	// TODO check that status code is in 2xx range?
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		forwarderStats.AddErrorMessage(elem.CompanyID, elem.EndPointId, resp.Status)
		return fmt.Errorf("forwardMg(%s): Bad status:%s companyId:%d endPointId:%d", devprod, resp.Status, elem.CompanyID, elem.EndPointId), true
	}

	return nil, false
}
