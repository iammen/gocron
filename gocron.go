// Package gocron : A Golang Job Scheduling Package.
//
// An in-process scheduler for periodic jobs that uses the builder pattern
// for configuration. Schedule lets you run Golang functions periodically
// at pre-determined intervals using a simple, human-friendly syntax.
//
// Inspired by the Ruby module clockwork <https://github.com/tomykaira/clockwork>
// and
// Python package schedule <https://github.com/dbader/schedule>
//
// See also
// http://adam.heroku.com/past/2010/4/13/rethinking_cron/
// http://adam.heroku.com/past/2010/6/30/replace_cron_with_clockwork/
//
// Copyright 2014 Jason Lyu. jasonlvhit@gmail.com .
// All rights reserved.
// Use of this source code is governed by a BSD-style .
// license that can be found in the LICENSE file.
package gocron

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// globals
var (
	// Time location, default set by the time.Local (*time.Location)
	loc                     = time.Local
	ErrTimeFormat           = errors.New("time format error")
	ErrParamsNotAdapted     = errors.New("the number of params is not adapted")
	ErrNotAFunction         = errors.New("only functions can be schedule into the job queue")
	ErrPeriodNotSpecified   = errors.New("unspecified job period")
	ErrParameterCannotBeNil = errors.New("nil paramaters cannot be used with reflection")
)

const (
	seconds = "seconds"
	minutes = "minutes"
	hours   = "hours"
	days    = "days"
	weeks   = "weeks"
)

// Job struct keeping information about job
type Job struct {
	mu         *sync.Mutex                // A mutex to safely access data across multiple goroutines
	interval   uint64                     // pause interval * unit bettween runs
	jobFunc    string                     // the job jobFunc to run, func[jobFunc]
	unit       string                     // time units, ,e.g. 'minutes', 'hours'...
	atTime     time.Duration              // optional time at which this job runs
	lastRun    time.Time                  // datetime of last run
	nextRun    time.Time                  // datetime of next run
	startDay   time.Weekday               // Specific day of the week to start on
	funcs      map[string]interface{}     // Map for the function task store
	fparams    map[string]([]interface{}) // Map for function and  params of function
	isRunning  bool                       // indicates that job is running
	err        error                      // Error
	lastResult *JobResult                 // Last job result
}

// JobResult struct.
type JobResult struct {
	result []reflect.Value
	err    error
}

// NewJob creates a new job with the time interval.
func NewJob(interval uint64) *Job {
	return &Job{
		mu:         new(sync.Mutex),
		interval:   interval,
		jobFunc:    "",
		unit:       "",
		atTime:     0,
		lastRun:    time.Unix(0, 0),
		nextRun:    time.Unix(0, 0),
		startDay:   time.Sunday,
		funcs:      make(map[string]interface{}),
		fparams:    make(map[string]([]interface{})),
		isRunning:  false,
		lastResult: &JobResult{},
	}
}

// At schedules job at specific time of day
// s.Every(1).Day().At("10:30").Do(task)
// s.Every(1).Monday().At("10:30").Do(task)
func (j *Job) At(t string) *Job {
	hour, min, err := formatTime(t)
	if err != nil {
		j.err = err
		return j
	}
	// save atTime start as duration from midnight
	j.atTime = time.Duration(hour)*time.Hour + time.Duration(min)*time.Minute
	return j
}

// Do specifies the jobFunc that should be called every time the job runs
func (j *Job) Do(jobFun interface{}, params ...interface{}) error {
	if j.err != nil {
		return j.err
	}

	typ := reflect.TypeOf(jobFun)
	if typ.Kind() != reflect.Func {
		return ErrNotAFunction
	}
	fname := getFunctionName(jobFun)

	j.mu.Lock()
	j.funcs[fname] = jobFun
	j.fparams[fname] = params
	j.jobFunc = fname
	j.mu.Unlock()

	j.scheduleNextRun()

	return nil
}

// Err should be checked to ensure an error didn't occur creating the job
func (j *Job) Err() error {
	return j.err
}

// NextScheduledTime returns the time of when this job is to run next
func (j *Job) NextScheduledTime() time.Time {
	j.mu.Lock()
	next := j.nextRun
	j.mu.Unlock()
	return next
}

// Seconds set the unit with seconds
func (j *Job) Seconds() *Job {
	return j.setUnit(seconds)
}

// Minutes set the unit with minute
func (j *Job) Minutes() *Job {
	return j.setUnit(minutes)
}

// Hours set the unit with hours
func (j *Job) Hours() *Job {
	return j.setUnit(hours)
}

// Days set the job's unit with days
func (j *Job) Days() *Job {
	return j.setUnit(days)
}

// Weeks sets the units as weeks
func (j *Job) Weeks() *Job {
	return j.setUnit(weeks)
}

// Second set the unit with second
func (j *Job) Second() *Job {
	j.mustInterval(1)
	return j.Seconds()
}

// Minute set the unit  with minute, which interval is 1
func (j *Job) Minute() *Job {
	j.mustInterval(1)
	return j.Minutes()
}

// Hour set the unit with hour, which interval is 1
func (j *Job) Hour() *Job {
	j.mustInterval(1)
	return j.Hours()
}

// Day set the job's unit with day, which interval is 1
func (j *Job) Day() *Job {
	j.mustInterval(1)
	return j.Days()
}

// Weekday start job on specific Weekday
func (j *Job) Weekday(startDay time.Weekday) *Job {
	j.mustInterval(1)
	j.startDay = startDay
	return j.Weeks()
}

// Monday set the start day with Monday
// - s.Every(1).Monday().Do(task)
func (j *Job) Monday() (job *Job) {
	return j.Weekday(time.Monday)
}

// Tuesday sets the job start day Tuesday
func (j *Job) Tuesday() *Job {
	return j.Weekday(time.Tuesday)
}

// Wednesday sets the job start day Wednesday
func (j *Job) Wednesday() *Job {
	return j.Weekday(time.Wednesday)
}

// Thursday sets the job start day Thursday
func (j *Job) Thursday() *Job {
	return j.Weekday(time.Thursday)
}

// Friday sets the job start day Friday
func (j *Job) Friday() *Job {
	return j.Weekday(time.Friday)
}

// Saturday sets the job start day Saturday
func (j *Job) Saturday() *Job {
	return j.Weekday(time.Saturday)
}

// Sunday sets the job start day Sunday
func (j *Job) Sunday() *Job {
	return j.Weekday(time.Sunday)
}

// the follow functions set the job's unit with seconds,minutes,hours...
func (j *Job) mustInterval(i uint64) error {
	if j.interval != i {
		return fmt.Errorf("interval maust be %d", i)
	}
	return nil
}

func (j *Job) periodDuration() (time.Duration, error) {
	interval := time.Duration(j.interval)
	switch j.unit {
	case seconds:
		return time.Duration(interval * time.Second), nil
	case minutes:
		return time.Duration(interval * time.Minute), nil
	case hours:
		return time.Duration(interval * time.Hour), nil
	case days:
		return time.Duration(interval * time.Hour * 24), nil
	case weeks:
		return time.Duration(interval * time.Hour * 24 * 7), nil
	}
	return interval, ErrPeriodNotSpecified
}

// roundToMidnight truncate time to midnight
func (j *Job) roundToMidnight(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
}

// Run the job and immdiately reschedule it
func (j *Job) run(results chan<- *JobResult) {
	f := reflect.ValueOf(j.funcs[j.jobFunc])
	params := j.fparams[j.jobFunc]
	if len(params) != f.Type().NumIn() {
		j.mu.Lock()
		j.lastResult.result = nil
		j.lastResult.err = ErrParamsNotAdapted
		j.isRunning = false
		j.mu.Unlock()
		results <- j.lastResult
	}

	var result []reflect.Value
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		// should check for nil items to avoid a panic
		if param == nil {
			j.mu.Lock()
			j.lastResult.result = nil
			j.lastResult.err = ErrParameterCannotBeNil
			j.isRunning = false
			j.mu.Unlock()
			results <- j.lastResult
		}
		in[k] = reflect.ValueOf(param)
	}

	// Set last run before running the job.
	// For fix issue: https://github.com/jasonlvhit/gocron/pull/57
	j.mu.Lock()
	j.lastRun = time.Now()
	j.isRunning = true
	j.mu.Unlock()

	result = f.Call(in)

	err := j.scheduleNextRun()
	if err != nil {
		j.mu.Lock()
		j.lastResult.result = nil
		j.lastResult.err = err
		j.isRunning = false
		j.mu.Unlock()
		results <- j.lastResult
	}

	j.mu.Lock()
	j.lastResult.result = result
	j.lastResult.err = nil
	j.isRunning = false
	j.mu.Unlock()
	results <- j.lastResult
}

// scheduleNextRun computes the instant when this job should run next
func (j *Job) scheduleNextRun() error {
	now := time.Now()
	if j.lastRun == time.Unix(0, 0) {
		j.mu.Lock()
		j.lastRun = now
		j.mu.Unlock()
	}

	switch j.unit {
	case days:
		j.mu.Lock()
		j.nextRun = j.roundToMidnight(j.lastRun)
		j.nextRun = j.nextRun.Add(j.atTime)
		j.isRunning = false
		j.mu.Unlock()
	case weeks:
		j.mu.Lock()
		j.nextRun = j.roundToMidnight(j.lastRun)
		dayDiff := int(j.startDay)
		dayDiff -= int(j.nextRun.Weekday())
		if dayDiff != 0 {
			j.nextRun = j.nextRun.Add(time.Duration(dayDiff) * 24 * time.Hour)
		}
		j.nextRun = j.nextRun.Add(j.atTime)
		j.isRunning = false
		j.mu.Unlock()
	default:
		j.mu.Lock()
		j.nextRun = j.lastRun
		j.isRunning = false
		j.mu.Unlock()
	}

	period, err := j.periodDuration()
	if err != nil {
		return err
	}

	// advance to next possible schedule
	for j.nextRun.Before(now) || j.nextRun.Before(j.lastRun) || j.nextRun.Equal(now) {
		j.mu.Lock()
		j.nextRun = j.nextRun.Add(period)
		j.isRunning = false
		j.mu.Unlock()
	}

	return nil
}

// setUnit sets unit type
func (j *Job) setUnit(unit string) *Job {
	j.mu.Lock()
	j.unit = unit
	j.mu.Unlock()
	return j
}

// True if the job should be run now
func (j *Job) shouldRun() bool {
	j.mu.Lock()
	b := time.Now().After(j.nextRun)
	j.mu.Unlock()
	return b && !j.isRunning
}

// Scheduler struct, the only data member is the list of jobs.
type Scheduler struct {
	err        error
	clear      bool
	mu         *sync.Mutex     // A Mutex exclusion lock
	workers    int             // Number of workers in pool
	jobs       []*Job          // Slice store jobs
	quit       chan struct{}   // Quit channel
	isQuit     bool            // Indicates that the scheduler is quit
	jobQueues  chan *Job       // The buffered channel that use to pass the running job to the worker.
	jobResults chan *JobResult // The buffered channel that use to pass the job results
}

// NewScheduler creates a new scheduler
func NewScheduler() *Scheduler {
	return &Scheduler{
		mu:     new(sync.Mutex),
		jobs:   []*Job{},
		isQuit: false,
	}
}

// Implements the sort.Interface{} for sorting jobs, by the time nextRun
func (s *Scheduler) Len() int {
	s.mu.Lock()
	l := len(s.jobs)
	s.mu.Unlock()
	return l
}

func (s *Scheduler) Swap(i, j int) {
	s.mu.Lock()
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
	s.mu.Unlock()
}

func (s *Scheduler) Less(i, j int) bool {
	s.mu.Lock()
	l := s.jobs[j].nextRun.After(s.jobs[i].nextRun)
	s.mu.Unlock()
	return l
}

// Clear delete all scheduled jobs
func (s *Scheduler) Clear() {
	s.mu.Lock()
	s.clear = true
	s.mu.Unlock()
}

// Every schedule a new periodic job with interval
// Use optional parameter for better understanding when calling a function
func (s *Scheduler) Every(intervals ...uint64) *Job {
	if len(intervals) == 0 {
		intervals = append(intervals, 1)
	}

	job := NewJob(intervals[0])
	s.mu.Lock()
	s.jobs = append(s.jobs, job)
	s.workers = len(s.jobs)
	s.mu.Unlock()
	return job
}

// Err should be checked to ensure an error didn't occur durning the scheduling
func (s *Scheduler) Err() error {
	return s.err
}

// NextRun datetime when the next job should run.
func (s *Scheduler) NextRun() (*Job, time.Time) {
	if len(s.jobs) <= 0 {
		return nil, time.Now()
	}
	sort.Sort(s)
	return s.jobs[0], s.jobs[0].nextRun
}

// RunPending runs all the jobs that are scheduled to run.
func (s *Scheduler) RunPending() error {
	if s.clear {
		s.mu.Lock()
		s.jobs = []*Job{}
		s.clear = false
		s.mu.Unlock()
		return nil
	}

	runnableJobs, n := s.getRunnableJobs()
	for i := 0; i < n; i++ {
		s.jobQueues <- runnableJobs[i]
	}

	return nil
}

// RunAll run all jobs regardless if they are scheduled to run or not
func (s *Scheduler) RunAll() {
	s.RunAllwithDelay(0)
}

// RunAllwithDelay runs all jobs with delay seconds
func (s *Scheduler) RunAllwithDelay(d int) {
	for i := 0; i < len(s.jobs); i++ {
		s.jobQueues <- s.jobs[i]
		if 0 != d {
			time.Sleep(time.Duration(d))
		}
	}
}

// Remove specific job j
func (s *Scheduler) Remove(j interface{}) {
	var nj []*Job
	for i := 0; i < len(s.jobs); i++ {
		if s.jobs[i].jobFunc == getFunctionName(j) {
			continue
		}
		nj = append(nj, s.jobs[i])
	}

	s.mu.Lock()
	s.jobs = nj
	s.mu.Unlock()
}

// Start all the pending jobs
// Add seconds ticker
func (s *Scheduler) Start() {
	s.quit = make(chan struct{}, 1)
	s.jobQueues = make(chan *Job, s.workers)
	s.jobResults = make(chan *JobResult, s.workers)
	ticker := time.NewTicker(100 * time.Millisecond)

	// Create worker pool.
	for wid := 1; wid <= s.workers; wid++ {
		go s.runJob(wid, s.jobQueues, s.jobResults)
	}

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				err := s.RunPending()
				if err != nil {
					s.err = err
					return
				}
			case r, ok := <-s.jobResults:
				if ok && r.err != nil {
					fmt.Printf("An error occurs: %s", r.err.Error())
				}
			case _, ok := <-s.quit:
				// Close quit channel if it still open.
				if ok {
					close(s.quit)
				}
				return
			}
		}
	}()

	return
}

// Quit gracfually stop the scheduler.
func (s *Scheduler) Quit() {
	// Send signal to scheduler to not generate next run.
	s.mu.Lock()
	s.isQuit = true
	s.mu.Unlock()

	// Wait until the job queues is empty.
	for {
		jobs := s.getRunningJobs()
		if jobs == 0 {
			break
		}

		fmt.Printf("Wait for %d jobs to finish before exiting.\n", jobs)
		time.Sleep(2 * time.Second)
	}

	// Send an empty struct to quit channel to leave.
	s.quit <- struct{}{}
	time.Sleep(time.Second)

	if err := s.Err(); err != nil {
		s.err = fmt.Errorf("Error occurs: %v", err)
	}

	s.Clear()
	fmt.Printf("All jobs has stopped.")

	return
}

// Get the current runnable jobs, which shouldRun is True
func (s *Scheduler) getRunnableJobs() ([]*Job, int) {
	var runnableJobs []*Job
	sort.Sort(s)
	for i := 0; i < len(s.jobs); i++ {
		if s.jobs[i].shouldRun() && !s.isQuit {
			runnableJobs = append(runnableJobs, s.jobs[i])
		}
	}
	return runnableJobs, len(runnableJobs)
}

// Get the running jobs
func (s *Scheduler) getRunningJobs() int {
	running := 0
	for i := 0; i < len(s.jobs); i++ {
		if s.jobs[i].isRunning {
			running++
		}
	}

	return running
}

// runJob runs the job that received from the job queue channel.
func (s *Scheduler) runJob(wid int, jobs <-chan *Job, results chan<- *JobResult) {
	for j := range jobs {
		j.run(results)
	}
}

// The following methods are shortcuts for not having to
// create a Schduler instance
var defaultScheduler = NewScheduler()

// ChangeLoc change default the time location
func ChangeLoc(newLocation *time.Location) {
	loc = newLocation
}

// Clear all scheduled jobs
func Clear() {
	defaultScheduler.Clear()
}

// Every schedules a new periodic job running in specific interval
// Use optional parameter for better understanding when calling a function
func Every(intervals ...uint64) *Job {
	if len(intervals) == 0 {
		intervals = append(intervals, 1)
	}

	return defaultScheduler.Every(intervals[0])
}

// Len gets the amount of jobs in the scheduler
func Len() int {
	return defaultScheduler.Len()
}

// NextRun gets the next running time
func NextRun() (job *Job, time time.Time) {
	return defaultScheduler.NextRun()
}

// Quit graceful stop the scheduler.
func Quit() {
	defaultScheduler.Quit()
	return
}

// Remove specific job
func Remove(j interface{}) {
	defaultScheduler.Remove(j)
}

// RunAll run all jobs regardless if they are scheduled to run or not.
func RunAll() {
	defaultScheduler.RunAll()
}

// RunAllwithDelay run all the jobs with a delay in seconds
// A delay of `delay` seconds is added between each job. This can help
// to distribute the system load generated by the jobs more evenly over
// time.
func RunAllwithDelay(d int) {
	defaultScheduler.RunAllwithDelay(d)
}

// RunPending run all jobs that are scheduled to run
// Please note that it is *intended behavior that run_pending()
// does not run missed jobs*. For example, if you've registered a job
// that should run every minute and you only call run_pending()
// in one hour increments then your job won't be run 60 times in
// between but only once.
func RunPending() {
	defaultScheduler.RunPending()
}

// Start run all jobs that are scheduled to run
func Start() {
	defaultScheduler.Start()
}

// for given function fn , get the name of funciton.
func getFunctionName(fn interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf((fn)).Pointer()).Name()
}

func formatTime(t string) (int, int, error) {
	var hour, min int

	ts := strings.Split(t, ":")
	if len(ts) != 2 {
		return hour, min, ErrTimeFormat
	}

	var err error
	if hour, err = strconv.Atoi(ts[0]); err != nil {
		return hour, min, err
	}

	if min, err = strconv.Atoi(ts[1]); err != nil {
		return hour, min, err
	}

	if hour < 0 || hour > 23 || min < 0 || min > 59 {
		return hour, min, ErrTimeFormat
	}

	return hour, min, nil
}
