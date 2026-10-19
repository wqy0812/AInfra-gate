package aigate

import "time"

// Shanghai has no DST; use an explicit location independent of host/container TZ.
var maintenanceLocation = time.FixedZone("Asia/Shanghai", 8*60*60)

func nextArchiveMaintenance(now time.Time) time.Time {
	local := now.In(maintenanceLocation)
	next := time.Date(local.Year(), local.Month(), local.Day(), 5, 0, 0, 0, maintenanceLocation)
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// One worker per owner, no startup catch-up and no overlapping passes.
func dailyArchiveMaintenance(stop <-chan struct{}, work func(time.Time)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			timer := time.NewTimer(time.Until(nextArchiveMaintenance(time.Now())))
			select {
			case <-stop:
				timer.Stop()
				return
			case now := <-timer.C:
				work(now)
			}
		}
	}()
	return done
}
