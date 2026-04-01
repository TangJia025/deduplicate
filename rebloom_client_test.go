package deduplicate

import (
	"regexp"
	"strconv"
	"testing"
	"time"
)

func TestGenerateKey(t *testing.T) {
	busId := "100"
	routeKey := "route123"

	k0 := generateKey(busId, 0, routeKey)
	k1 := generateKey(busId, 1, routeKey)

	// 1. 格式校验：busId_yyyymmdd_partition
	re := regexp.MustCompile(`^\d+_\d{8}_\d+$`)
	if !re.MatchString(k0) {
		t.Fatalf("generateKey(%q,0,%q) format invalid, got %q", busId, routeKey, k0)
	}
	if !re.MatchString(k1) {
		t.Fatalf("generateKey(%q,1,%q) format invalid, got %q", busId, routeKey, k1)
	}

	// 2. 日期递减验证
	parts0 := regexp.MustCompile("_").Split(k0, -1)
	parts1 := regexp.MustCompile("_").Split(k1, -1)
	if len(parts0) != 3 || len(parts1) != 3 {
		t.Fatalf("unexpected split parts: %v %v", parts0, parts1)
	}

	date0 := parts0[1]
	date1 := parts1[1]
	expectedDate0 := time.Now().Format("20060102")
	if date0 != expectedDate0 {
		t.Fatalf("expected day0 %s, got %s", expectedDate0, date0)
	}

	expectedDate1 := time.Now().AddDate(0, 0, -1).Format("20060102")
	if date1 != expectedDate1 {
		t.Fatalf("expected day1 %s, got %s", expectedDate1, date1)
	}

	// 3. 同一个busId+routeKey同i值应该分区一致
	k0b := generateKey(busId, 0, routeKey)
	if k0 != k0b {
		t.Fatalf("generateKey not stable for same input: %s vs %s", k0, k0b)
	}

	// 4. 分区值范围
	partition0, err := strconv.Atoi(parts0[2])
	if err != nil {
		t.Fatalf("parse partition failed: %v", err)
	}
	if partition0 < 0 || partition0 >= PartitionNum {
		t.Fatalf("partition out of range: %d", partition0)
	}
}
