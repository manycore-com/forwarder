package forwarderpubsub

import (
	pubsub "cloud.google.com/go/pubsub"
	"fmt"
	"testing"
)


func TestAgeInSecMessage(t *testing.T) {
	var msg pubsub.Message

	fmt.Printf("nsg %v\n", msg)
}
