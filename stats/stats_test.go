package stats

import (
	"fmt"
	"testing"
)

func TestGetMemUsageStr(t *testing.T) {
	fmt.Printf("%s\n", GetMemUsageStr())
}
