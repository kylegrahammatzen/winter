// Graceful shutdown example: a job that finishes while the server is stopping is
// durably acked, so it is not re-run on the next start.
//
// Run with: go run ./examples/graceful-shutdown
// Press ctrl+c while the report is processing. The handler runs to completion,
// the server drains it, and the ack lands even though the context was cancelled.
// Run it again and the job does not reprocess.
// Requires Redis running on localhost:6379.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kylegrahammatzen/winter"
)

type ProcessReport struct {
	ReportID string `json:"report_id"`
}

func (ProcessReport) Kind() string { return "report.process" }

func main() {
	redisCfg := winter.RedisConfig{Addr: "localhost:6379"}

	client, err := winter.NewClient(redisCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	job, err := winter.Enqueue(client, context.Background(), ProcessReport{ReportID: "rpt-001"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued job %s, press ctrl+c while it runs then start again to confirm it stays done\n", job.ID)

	server, err := winter.NewServer(redisCfg, winter.ServerConfig{
		Concurrency: 1,
		Queues:      winter.Queues("default", 1),
	})
	if err != nil {
		log.Fatal(err)
	}

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[ProcessReport]) error {
		// This job must finish once started, so it ignores cancellation and lets the
		// server drain it. Winter acks the finished job on a shutdown-surviving context.
		for step := 1; step <= 3; step++ {
			time.Sleep(time.Second)
			fmt.Printf("report %s step %d/3\n", job.Args.ReportID, step)
		}
		fmt.Printf("report %s done\n", job.Args.ReportID)
		return nil
	})

	server.Use(winter.Recover())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		<-sig
		server.Stop()
	}()

	fmt.Println("server starting, press ctrl+c to stop")
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
