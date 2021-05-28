package webhook

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func setEnv() error {
	os.Setenv("SIMPLE_HASH_PASSWORD", "bob hund")
	return env()
}

func TestCalculateUnsafeHash(t *testing.T) {
	assert.Equal(t, "89c048d699972e3bdc33f8f102c3e605", CalculateUnsafeHash("bob hund" + "1"))
}

func TestValidateUrlPath(t *testing.T) {
	setEnv()

	tests := []struct {
		path string
		esp string
		isOk bool
		version int
		safeHash string
	}{
		{path: "/snor/sg/1/89c048d699972e3bdc33f8f102c3e605/afa807767df324408a8b825ca8303a37/", esp: "sg", isOk: false, version: 1, safeHash: "afa807767df324408a8b825ca8303a37"},
		{path: "/v1/sg/1/89c048d699972e3bdc33f8f102c3e605/y/", esp: "sg", isOk: true, version: 1, safeHash: "y"},
		{path: "/v1/hi-there/1/89c048d699972e3bdc33f8f102c3e605/z/", esp: "", isOk: false, version: 1, safeHash: "z"},
		{path: "/v1/sg/aaaa/1/x/", esp: "", isOk: false, version: 1, safeHash: "x"},
		{path: "/v1/sg/1/89c048d699972e3bdc33f8f102c3e605/y/", esp: "sg", isOk: true, version: 1, safeHash: "y"},
	}

	for _, test := range tests {
		version, _, esp, safeHash, err := ValidateUrlPath(test.path)

		if ! test.isOk {
			if err == nil {
				t.Errorf("TestF(%q) = expected test case failure, got no error", test.path)
			} else {
				continue
			}
		}

		if test.isOk && err != nil {
			t.Errorf("TestF(%q) = expected test case ok, got error %v", test.path, err)
		}

		if test.version != version {
			t.Errorf("TestF(%q) expected version=%v, got %v", test.path, test.version, version)
		}

		if test.esp != "" {
			if test.esp != esp {
				t.Errorf("TestF(%q) = expected esp=%s, got %s", test.path, test.esp, esp)
			}
		}

		if test.safeHash != safeHash {
			t.Errorf("TestF(%q) = expected safeHash=%q, got %q", test.path, test.safeHash, safeHash)
		}
	}

}


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

func TestValidateBody(t *testing.T) {

	tests := []struct {
		body string
		isOk bool
	}{
		{body: okPayload, isOk: true},
		{body: `{"name": "Gopher"}`, isOk:false},
	}

	for _, test := range tests {
		req := httptest.NewRequest("POST", "/", strings.NewReader(test.body))
		req.Header.Add("Content-Type", "application/json")

		// string(b)
		_, err := ValidateBody(req)

		if test.isOk && err != nil {
			t.Errorf("TestF(%q) = expected ok, error %v", test.body, err)
		}

		if ! test.isOk && err == nil {
			t.Errorf("TestF(%q) = expected nok, error nil", test.body)
		}
	}
}

func TestExtractSign(t *testing.T) {

	tests := []struct {
		cookieKey string
		cookieVal string
		esp       string
		wantVal   string
		isOk      bool
	} {
		{"apa", "banan", "sg", "", true},
		{"x-twilio-Email-Event-Webhook-Signature", "banan", "sg", "banan", true},
		{"X-Twilio-Email-Event-Webhook-Signature", "banan", "sg", "banan", true},
		{"X-Twilio-Email-Event-Webhook-Signature", "banan", "borkyMcBorkFace", "", false},
	}

	for _, test := range tests {
		req := httptest.NewRequest("POST", "/", nil)
		req.Header.Add(test.cookieKey, test.cookieVal)

		sign, err := ExtractSign(req, test.esp)

		if ! test.isOk {
			// Expect failure, but error is nil
			if err == nil {
				t.Errorf("TestExtractSign(%v) = expected nok, error nil", test)
			}

			continue
		}

		// all is ok
		if sign != test.wantVal {
			t.Errorf("TestExtractSign(%v) = expected sign=%q, got %q", test, test.wantVal, sign)
		}
	}
}

func TestFDev(t *testing.T) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("FORWARDER_TEST_GOOGLE_APPLICATION_CREDENTIALS"))
	os.Setenv("PROJECT_ID", os.Getenv("FORWARDER_TEST_PROJECT_ID"))
	os.Setenv("OUT_QUEUE_TOPIC_ID", os.Getenv("FORWARDER_TEST_RESPONDER_SUBS"))
	os.Setenv("OUT_TRIGGER_TOPIC_ID", os.Getenv("FORWARDER_TEST_RESPONDER_TRG"))
	os.Setenv("TRIGGER_RATIO", "1")
	os.Setenv("SIMPLE_HASH_PASSWORD", "bob hund") //  os.Getenv("FORWARDER_TEST_SIMPLE_HASH_PASSWORD"))

	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req := httptest.NewRequest("POST", "/v1/sg/1/89c048d699972e3bdc33f8f102c3e605/y/", strings.NewReader(okPayload))
	req.Header.Add("Content-Type", "application/json")

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(F)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func testMemLeakInner() error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, os.Getenv("FORWARDER_TEST_PROJECT_ID"))  // client
	if err != nil {
		return fmt.Errorf("error: Failed to instantiate Client: %v\n", err)
	}
	if nil != client {
		defer client.Close()
	}

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


	outQueueTopic := client.Topic("TESTING")
	outQueueResult := outQueueTopic.Publish(ctx, &pubsub.Message{
		Data: []byte(okPayload),
	})

	// First, wait for the outQueue since we sent to that first
	_, waitErr := outQueueResult.Get(ctx)
	if waitErr != nil {
		return fmt.Errorf("error: Failed to send to %s pubsub: %v\n", outQueueTopicId, waitErr)
	}

	return nil
}


func TestMemLeak(t *testing.T) {

	fmt.Printf("mem start: %s\n", forwarderStats.GetMemUsageStr())

	for i:=0; i<100; i++ {

		req := httptest.NewRequest("POST", "/", strings.NewReader(okPayload))

		_, err := ValidateBody(req)
		if nil != err {
			fmt.Printf("Error: %v\n", err)
			return
		}

		fmt.Printf("mem after: %s\n", forwarderStats.GetMemUsageStr())

	}


}
