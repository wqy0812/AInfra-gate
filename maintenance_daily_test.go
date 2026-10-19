package aigate

import (
	"testing"
	"time"
)

func TestDailyMaintenanceShanghaiNoCatchup(t *testing.T) {
	for _, test := range []struct{ now, next string }{
		{"2026-10-19T20:59:59Z", "2026-10-19T21:00:00Z"},
		{"2026-10-19T21:00:00Z", "2026-10-20T21:00:00Z"},
		{"2026-10-19T23:00:00Z", "2026-10-20T21:00:00Z"},
	} {
		now, _ := time.Parse(time.RFC3339, test.now)
		if got := nextArchiveMaintenance(now).UTC().Format(time.RFC3339); got != test.next {
			t.Fatal(got, test.next)
		}
	}
	stop := make(chan struct{})
	ran := make(chan struct{}, 1)
	done := dailyArchiveMaintenance(stop, func(time.Time) { ran <- struct{}{} })
	close(stop)
	<-done
	select {
	case <-ran:
		t.Fatal("startup performed maintenance")
	default:
	}
}
