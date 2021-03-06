package gocron

import (
	"fmt"
	"testing"
	"time"
)

func task() {
	fmt.Println("I am a running job.")
}

func taskWithParams(a int, b string) {
	fmt.Println(a, b)
}

func assertEqualTime(name string, t *testing.T, actual, expected time.Time) {
	if !actual.Equal(expected) {
		t.Errorf("test name: %s actual different than expected want: %v -> got: %v", name, expected, actual)
	}
}

func TestSecond(t *testing.T) {
	defaultScheduler.Every().Second().Do(task)
	defaultScheduler.Every().Second().Do(taskWithParams, 1, "hello")
	defaultScheduler.Start()
	time.Sleep(2 * time.Second)
	defaultScheduler.Quit()

	if err := defaultScheduler.Err(); err != nil {
		t.Error(err)
	}
	defaultScheduler.Clear()
}

func Test_formatTime(t *testing.T) {
	tests := []struct {
		name     string
		args     string
		wantHour int
		wantMin  int
		wantErr  bool
	}{
		{
			name:     "normal",
			args:     "16:18",
			wantHour: 16,
			wantMin:  18,
			wantErr:  false,
		},
		{
			name:     "normal",
			args:     "6:18",
			wantHour: 6,
			wantMin:  18,
			wantErr:  false,
		},
		{
			name:     "notnumber",
			args:     "e:18",
			wantHour: 0,
			wantMin:  0,
			wantErr:  true,
		},
		{
			name:     "outofrange",
			args:     "25:18",
			wantHour: 25,
			wantMin:  18,
			wantErr:  true,
		},
		{
			name:     "wrongformat",
			args:     "19:18:17",
			wantHour: 0,
			wantMin:  0,
			wantErr:  true,
		},
		{
			name:     "wrongminute",
			args:     "19:1e",
			wantHour: 19,
			wantMin:  0,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHour, gotMin, err := formatTime(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHour != tt.wantHour {
				t.Errorf("formatTime() gotHour = %v, want %v", gotHour, tt.wantHour)
			}
			if gotMin != tt.wantMin {
				t.Errorf("formatTime() gotMin = %v, want %v", gotMin, tt.wantMin)
			}
		})
	}
}

func TestTaskAt(t *testing.T) {
	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule to run in next minute
	now := time.Now()
	// Expected start time
	startTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+1, 0, 0, loc)
	fmt.Printf("Start time: %v\n", startTime)

	// Expected next start time day after
	startNext := startTime.AddDate(0, 0, 1)

	// Schedule every day At
	startAt := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute()+1)
	dayJob := s.Every().Day().At(startAt)
	if err := dayJob.Err(); err != nil {
		t.Error(err)
	}

	dayJobDone := make(chan bool, 1)
	// Job running 5 sec
	dayJob.Do(func() {
		t.Log(time.Now(), "job start")
		time.Sleep(2 * time.Second)
		dayJobDone <- true
		t.Log(time.Now(), "job done")
	})

	// Check first run
	dayJob.scheduleNextRun()
	assertEqualTime("first run", t, startTime, dayJob.nextRun)

	s.Start()               // Start scheduler
	<-dayJobDone            // Wait job done
	s.Quit()                // Stop scheduler
	time.Sleep(time.Second) // wait for scheduler to reschedule job

	// Check next run
	nextRun := dayJob.NextScheduledTime()
	assertEqualTime("next run", t, nextRun, startNext)
}

func TestDaily(t *testing.T) {
	now := time.Now()

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// schedule next run 1 day
	dayJob := s.Every().Day()
	dayJob.scheduleNextRun()
	exp := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("1 day", t, dayJob.nextRun, exp)

	// schedule next run 2 days
	dayJob = s.Every(2).Days()
	dayJob.scheduleNextRun()
	exp = time.Date(now.Year(), now.Month(), now.Day()+2, 0, 0, 0, 0, loc)
	assertEqualTime("2 days", t, dayJob.nextRun, exp)

	// Job running longer than next schedule 1day 2 hours
	dayJob = s.Every().Day()
	dayJob.lastRun = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+2, 0, 0, 0, loc)
	dayJob.scheduleNextRun()
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("1 day 2 hours", t, dayJob.nextRun, exp)

	// At() 2 hours before now
	hour := now.Hour() - 2
	minute := now.Minute()
	startAt := fmt.Sprintf("%02d:%02d", hour, minute)
	dayJob = s.Every().Day().At(startAt)
	if err := dayJob.Err(); err != nil {
		t.Error(err)
	}

	dayJob.scheduleNextRun()
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, loc)
	assertEqualTime("at 2 hours before now", t, dayJob.nextRun, exp)
}

func TestWeekdayAfterToday(t *testing.T) {
	now := time.Now()

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule job at next week day
	var weekJob *Job
	switch now.Weekday() {
	case time.Monday:
		weekJob = s.Every().Tuesday()
	case time.Tuesday:
		weekJob = s.Every().Wednesday()
	case time.Wednesday:
		weekJob = s.Every().Thursday()
	case time.Thursday:
		weekJob = s.Every().Friday()
	case time.Friday:
		weekJob = s.Every().Saturday()
	case time.Saturday:
		weekJob = s.Every().Sunday()
	case time.Sunday:
		weekJob = s.Every().Monday()
	}

	// First run
	weekJob.scheduleNextRun()
	exp := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("first run", t, weekJob.nextRun, exp)

	// Simulate job run 7 days before
	weekJob.lastRun = weekJob.nextRun.AddDate(0, 0, -7)
	// Next run
	weekJob.scheduleNextRun()
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("next run", t, weekJob.nextRun, exp)
}

func TestWeekdayBeforeToday(t *testing.T) {
	now := time.Now()

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule job at day before
	var weekJob *Job
	switch now.Weekday() {
	case time.Monday:
		weekJob = s.Every().Sunday()
	case time.Tuesday:
		weekJob = s.Every().Monday()
	case time.Wednesday:
		weekJob = s.Every().Tuesday()
	case time.Thursday:
		weekJob = s.Every().Wednesday()
	case time.Friday:
		weekJob = s.Every().Thursday()
	case time.Saturday:
		weekJob = s.Every().Friday()
	case time.Sunday:
		weekJob = s.Every().Saturday()
	}

	weekJob.scheduleNextRun()
	exp := time.Date(now.Year(), now.Month(), now.Day()+6, 0, 0, 0, 0, loc)
	assertEqualTime("first run", t, weekJob.nextRun, exp)

	// Simulate job run 7 days before
	weekJob.lastRun = weekJob.nextRun.AddDate(0, 0, -7)
	// Next run
	weekJob.scheduleNextRun()
	exp = time.Date(now.Year(), now.Month(), now.Day()+6, 0, 0, 0, 0, loc)
	assertEqualTime("nest run", t, weekJob.nextRun, exp)
}

func TestWeekdayAt(t *testing.T) {
	now := time.Now()

	hour := now.Hour()
	minute := now.Minute()
	startAt := fmt.Sprintf("%02d:%02d", hour, minute)

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule job at next week day
	var weekJob *Job
	switch now.Weekday() {
	case time.Monday:
		weekJob = s.Every().Tuesday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Tuesday:
		weekJob = s.Every().Wednesday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Wednesday:
		weekJob = s.Every().Thursday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Thursday:
		weekJob = s.Every().Friday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Friday:
		weekJob = s.Every().Saturday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Saturday:
		weekJob = s.Every().Sunday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Sunday:
		weekJob = s.Every().Monday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	}

	// First run
	weekJob.scheduleNextRun()
	exp := time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, loc)
	assertEqualTime("first run", t, weekJob.nextRun, exp)

	// Simulate job run 7 days before
	weekJob.lastRun = weekJob.nextRun.AddDate(0, 0, -7)
	// Next run
	weekJob.scheduleNextRun()
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, loc)
	assertEqualTime("next run", t, weekJob.nextRun, exp)
}
