package deduplicate

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var keyFormatRE = regexp.MustCompile(`^\d+_\d{8}_\d+$`)

func TestGenerateKey(t *testing.T) {
	busId := "100"
	routeKey := "route123"
	now := time.Now()

	k0 := generateKey(busId, 0, routeKey)
	k1 := generateKey(busId, 1, routeKey)

	if !keyFormatRE.MatchString(k0) {
		t.Fatalf("generateKey(%q,0,%q) format invalid, got %q", busId, routeKey, k0)
	}
	if !keyFormatRE.MatchString(k1) {
		t.Fatalf("generateKey(%q,1,%q) format invalid, got %q", busId, routeKey, k1)
	}

	parts0 := strings.Split(k0, "_")
	parts1 := strings.Split(k1, "_")

	expectedDate0 := now.Format("20060102")
	if parts0[1] != expectedDate0 {
		t.Fatalf("expected day0 %s, got %s", expectedDate0, parts0[1])
	}

	expectedDate1 := now.AddDate(0, 0, -1).Format("20060102")
	if parts1[1] != expectedDate1 {
		t.Fatalf("expected day1 %s, got %s", expectedDate1, parts1[1])
	}

	k0b := generateKey(busId, 0, routeKey)
	if k0 != k0b {
		t.Fatalf("generateKey not stable for same input: %s vs %s", k0, k0b)
	}

	for i, parts := range [][]string{parts0, parts1} {
		partition, err := strconv.Atoi(parts[2])
		if err != nil {
			t.Fatalf("parse partition of k%d failed: %v", i, err)
		}
		if partition < 0 || partition >= PartitionNum {
			t.Fatalf("partition out of range for k%d: %d", i, partition)
		}
	}
}
