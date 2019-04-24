package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/iammen/gocron"
)

func task() {
	fmt.Printf("I am runnning task.: %v\n", time.Now().Format("2006-01-02 15:04:05"))
}

func taskWithParams(a int, b string) {
	fmt.Println(a, b)
}

func main() {
	// Start all jobs.
	s := gocron.NewScheduler()
	s.Every().Minute().Do(taskWithParams, 1, "hello")
	s.Every(5).Seconds().Do(task)
	stopped := s.Start()

	// Wait for interrupt signal to gracefully stop all jobs.
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	fmt.Printf("Please wait a moment.\n")

	// Gracefully stop all running jobs.
	// stopped <- struct{}{}
	// close(stopped)
	// time.Sleep(2 * time.Second)

	defer s.Stop(stopped)
}
