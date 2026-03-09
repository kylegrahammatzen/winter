// Worker example: processes tasks from multiple weighted queues.
//
// Run with: go run ./examples/multi-queue/worker
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
	"github.com/kylegrahammatzen/winter/examples/multi-queue/tasks"
)

func main() {
	server, err := winter.NewServer(
		winter.RedisConfig{Addr: "localhost:6379"},
		winter.ServerConfig{
			Concurrency: 20,
			// Payments get 6x the poll weight, default gets 3x, low gets 1x.
			Queues: winter.Queues("payments", 6, "default", 3, "low", 1),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[tasks.ProcessPayment]) error {
		fmt.Printf("processing payment %s for $%.2f\n", job.Args.PaymentID, float64(job.Args.Amount)/100)
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[tasks.SendNotification]) error {
		fmt.Printf("notifying user %d: %s\n", job.Args.UserID, job.Args.Message)
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[tasks.GenerateReport]) error {
		fmt.Printf("generating %s report\n", job.Args.ReportType)
		return nil
	})

	server.Use(winter.Recover())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		<-sig
		server.Stop()
	}()

	fmt.Println("worker starting, press ctrl+c to stop")
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
