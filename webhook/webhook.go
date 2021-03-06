package webhook

import (
	pubsub "cloud.google.com/go/pubsub"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderEsp "github.com/manycore-com/forwarder/esp"
	forwarderPubsub "github.com/manycore-com/forwarder/pubsub"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Listens to requests on a webhook
// Does a very quick verification, and then it puts them on an internal queue

// We push everything over environment variables.
// You can, instead of using this F() method directly, create your own package, add entry functions,
// os.Setenv() as you see fit, and then call F() here.
var projectId = ""
var outQueueTopicId = ""
var simpleHashPassword = ""
var devprod = ""  // Optional: We use dev for development, devprod for live test, prod for live
func env() error {
	projectId = os.Getenv("PROJECT_ID")
	outQueueTopicId = os.Getenv("OUT_QUEUE_TOPIC_ID")
	simpleHashPassword = os.Getenv("SIMPLE_HASH_PASSWORD")

	if projectId == "" {
		return fmt.Errorf("missing PROJECT_ID environment variable")
	}

	if outQueueTopicId == "" {
		return fmt.Errorf("missing OUT_QUEUE_TOPIC_ID environment variable")
	}

	if simpleHashPassword == "" {
		return fmt.Errorf("missing SIMPLE_HASH_PASSWORD environment variable")
	}

	devprod = os.Getenv("DEV_OR_PROD")

	return nil
}

func CalculateUnsafeHash(secret string) string {
	var calculatedHash = fmt.Sprintf("%x", sha256.Sum256([]byte(secret)))
	hashHead := calculatedHash[0:32]
	return hashHead
}

// ValidateUrlPath is tested for memory leaks
// There are two hashes. This is just a quick check to filter out bots and what not.
// ValidateUrlPath we expect /v1/sg/1/08491a2c7c145127f83ac9654264cbe7/x/
// -> version int, companyId int, simpleHash string, safeHash string, error error
const offsetUriVersion = 0
const offsetUriEsp = 1
const offsetUriCompanyId = 2
const offsetUriUnsafeHash = 3
const offsetUriSafeHash = 4
func ValidateUrlPath(path string) (int, int, string, string, error) {
	var companyId int = -1
	var esp = ""
	var version = -1
	var safeHash = ""

	var splitFn = func(c rune) bool {
		return c == '/'
	}
	var splitted = strings.FieldsFunc(path, splitFn)

	if 5 != len(splitted) {
		errStr := "error: bad path. Expects /v1/sg/<int:company_id>/<str:hash>/f0/"
		return -1, -1, esp, safeHash, errors.New(errStr)
	}

	if "v1" != splitted[offsetUriVersion] {
		errStr := "error: Version is not ok. The URI should start with /v1/"
		return -1, -1, esp, safeHash, errors.New(errStr)
	}
	version = 1


	esp = splitted[offsetUriEsp]

	if ! forwarderEsp.IsOkEsp(esp) {
		errStr := fmt.Sprintf("error: bad path. Unsupported esp:%s. Allowed values:%s. URI example: /sg/<int:company_id>/<str:hash>/f0/", esp, forwarderEsp.SupportedEsp())
		return -1, -1, esp, safeHash, errors.New(errStr)
	}

	companyId, err := strconv.Atoi(splitted[offsetUriCompanyId])
	if err != nil {
		errStr := "error: second word in path is not an integer"
		return -1, -1, esp, safeHash, errors.New(errStr)
	}

	hashHead := CalculateUnsafeHash(simpleHashPassword + splitted[offsetUriCompanyId])

	if hashHead != splitted[offsetUriUnsafeHash] {
		errStr := "error: bad hash, got " + splitted[offsetUriUnsafeHash]
		return -1, -1, esp, safeHash, errors.New(errStr)
	}

	safeHash = splitted[offsetUriSafeHash]

	return version, companyId, esp, safeHash, nil
}

func ValidateBody(r *http.Request) ([]byte, error) {
	var b []byte = nil
	var err error = nil

	// Read body
	b, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// Parse body
	var objmap []map[string]interface{}
	if err := json.Unmarshal(b, &objmap); err != nil {
		return nil, err
	}

	return b, nil
}

func ExtractSign(r *http.Request, esp string) (string, error) {
	if ! forwarderEsp.IsOkEsp(esp) {
		return "", fmt.Errorf("extractSign: invalid esp: %s", esp)
	}

	var header = forwarderEsp.GetSignHeaderName(esp)

	if "" == header {
		return "", nil
	} else {
		return r.Header.Get("X-Twilio-Email-Event-Webhook-Signature"), nil
	}
}

// Send is tested for memory leaks
func Send(payload []byte) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)  // client
	if err != nil {
		return fmt.Errorf("error: Failed to instantiate Client: %v\n", err)
	}
	if nil != client {
		defer client.Close()
	}

	outQueueTopic := client.Topic(outQueueTopicId)
	outQueueResult := outQueueTopic.Publish(ctx, &pubsub.Message{
		Data: payload,
	})

	// First, wait for the outQueue since we sent to that first
	_, waitErr := outQueueResult.Get(ctx)
	if waitErr != nil {
		return fmt.Errorf("error: Failed to send to %s pubsub: %v\n", outQueueTopicId, waitErr)
	}

	return nil
}

func cleanup() {
	forwarderStats.CleanupV2()
}

func F(w http.ResponseWriter, r *http.Request) {
	defer cleanup()

	err := env()
	if nil != err {
		http.Error(w, "Webhook responder is mis configured", http.StatusInternalServerError)
		fmt.Printf("Error: %v\n", err)
		return
	}

	_, companyId, esp, safeHash, pathErr := ValidateUrlPath(r.URL.Path)
	if nil != pathErr {
		fmt.Printf("Error: Url is invalid: %v\n", pathErr)
		http.Error(w, pathErr.Error(), http.StatusUnauthorized)
		return
	}

	b, bodyErr := ValidateBody(r)
	if nil != bodyErr {
		fmt.Printf("Error: Body is invalid: %v\n", bodyErr)
		http.Error(w, bodyErr.Error(), http.StatusBadRequest)
		return
	}

	rand.Seed(time.Now().UnixNano())  // Rid in PubSubElement also needs Seed
	if 0 == rand.Intn(100) {
		fmt.Printf("Package (rand 1/100 for debug): %v\n", string(b))
	}

	// The ESP sign cookie.
	sign, signErr := ExtractSign(r, esp)
	if signErr != nil {
		fmt.Printf("Error: Failed to extract sign: %v\n", signErr)
		http.Error(w, fmt.Sprintf("failed to extract sign: %v", signErr), http.StatusInternalServerError)
		return
	}

	structToPush := forwarderPubsub.PubSubElement{
		CompanyID: companyId,
		ESP: esp,
		ESPJsonString: string(b),
		Ts: time.Now().Unix(),
		SafeHash: safeHash,
		Sign: sign,
		Rid: int(rand.Int31()),  // Seed set above
	}

	payload, err := json.Marshal(structToPush)
	if err != nil {
		fmt.Printf("Error: Failed to Marshal pubsub payload: %v\n", err)
		http.Error(w, fmt.Sprintf("marshal of payload failed: %v", err), http.StatusInternalServerError)
		return
	}

	err = Send(payload)
	if err != nil {
		fmt.Printf("Error: Failed to push to pubsub: %v\n", err)
		http.Error(w, fmt.Sprintf("failed to push to pubsub: %v", err), http.StatusInternalServerError)
		return
	}

	var memUsage = forwarderStats.GetMemUsageStr()
	fmt.Printf("forwarder.webhook.F(%s) ok. v%s CompanyId:%d, Memstats: %s\n", devprod, forwarderCommon.PackageVersion, companyId, memUsage)

	fmt.Fprintf(w, "ok (%s)", memUsage)
}
