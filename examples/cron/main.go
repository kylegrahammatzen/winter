// Cron example: schedule periodic jobs with cron expressions.
//
// Run with: go run ./examples/cron
// Requires Redis running on localhost:6379.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kylegrahammatzen/winter"
)

type CleanupExpiredSessions struct{}

func (CleanupExpiredSessions) Kind() string { return "cleanup.sessions" }

type GenerateDailyReport struct {
	ReportType string `json:"report_type"`
}

func (GenerateDailyReport) Kind() string { return "report.daily" }

func main() {
	server, err := winter.NewServer(
		winter.RedisConfig{Addr: "localhost:6379"},
		winter.ServerConfig{
			Concurrency: 5,
			Queues:      winter.Queues("default", 1, "maintenance", 1),
			Cron: []winter.CronEntry{
				{
					Name:     "session-cleanup",
					Schedule: "*/1 * * * *",
					Kind:     "cleanup.sessions",
					Queue:    "maintenance",
				},
				{
					Name:     "daily-report",
					Schedule: "0 9 * * *",
					Kind:     "report.daily",
					Queue:    "default",
					Payload:  []byte(`{"report_type":"daily-summary"}`),
				},
			},
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[CleanupExpiredSessions]) error {
		fmt.Println("cleaning up expired sessions")
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[GenerateDailyReport]) error {
		fmt.Printf("generating %s\n", job.Args.ReportType)
		return nil
	})

	server.Use(winter.Recover())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		<-sig
		server.Stop()
	}()

	fmt.Println("server starting with cron jobs, press ctrl+c to stop")
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
