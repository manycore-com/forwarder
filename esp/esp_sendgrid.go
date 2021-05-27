package esp

import (
	"bytes"
	"fmt"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"io"
	"net/http"
)



func ForwardSg(devprod string, elem *forwarderPubsub.PubSubElement) (error, bool) {  // bool: any point to retry

	if "" == elem.Dest {
		// This should never never happen
		forwarderStats.AddErrorMessage(elem.CompanyID, "Missing destination URL.")
		return fmt.Errorf("forwarder.forward.forwardMg(%s): Missing Dest url", devprod), false
	}

	// ok, time to forward
	request, err := http.NewRequest("POST", elem.Dest, bytes.NewReader([]byte(elem.ESPJsonString)))
	if err != nil {
		forwarderStats.AddErrorMessage(elem.CompanyID, err.Error())
		return err, true
	}

	request.Header.Set("Content-Type", "application/json")

	// FIXME Decide if we make the verification or if we just defer it like this

	// For Sendgrid we know it's going to be X-Twilio-Email-Event-Webhook-Signature
	var header = GetSignHeaderName("sg")
	if "" != header && "" != elem.Sign {
		request.Header.Set(header, elem.Sign)
	}

	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		forwarderStats.AddErrorMessage(elem.CompanyID, err.Error())
		if resp == nil {
			return err, false
		} else {
			return err, resp.StatusCode >= 500
		}
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			forwarderStats.AddErrorMessage(elem.CompanyID, err.Error())
			fmt.Printf("forwarder.forward.forwardMg(%s): Error closing Body:%v\n", devprod, err)
		}
	} (resp.Body)

	fmt.Printf("forwarder.forward.forwardMg(%s): ok. Status:%v\n", devprod, resp.Status)

	// TODO check that status code is in 2xx range?
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		forwarderStats.AddErrorMessage(elem.CompanyID, resp.Status)
		return fmt.Errorf("forwardMg(%s): Bad status:%s companyId:%d url:%s", devprod, resp.Status, elem.CompanyID, elem.Dest), true
	}

	return nil, false
}
