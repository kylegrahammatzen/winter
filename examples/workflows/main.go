// Workflows example: demonstrates Chain, Group, and Chord patterns.
//
// Run with: go run ./examples/workflows
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

type ProcessOrder struct {
	OrderID string `json:"order_id"`
}

func (ProcessOrder) Kind() string { return "order.process" }

type GenerateInvoice struct {
	OrderID string `json:"order_id"`
}

func (GenerateInvoice) Kind() string { return "invoice.generate" }

type SendReceipt struct {
	OrderID string `json:"order_id"`
}

func (SendReceipt) Kind() string { return "receipt.send" }

type Build struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

func (Build) Kind() string { return "build" }

type Deploy struct {
	Version string `json:"version"`
}

func (Deploy) Kind() string { return "deploy" }

func main() {
	redisCfg := winter.RedisConfig{Addr: "localhost:6379"}

	client, err := winter.NewClient(redisCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Chain: process order -> generate invoice -> send receipt.
	chainID, err := winter.Chain(client, ctx, []winter.Task{
		ProcessOrder{OrderID: "ord-123"},
		GenerateInvoice{OrderID: "ord-123"},
		SendReceipt{OrderID: "ord-123"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("chain created: %s\n", chainID)

	// Group: build for all platforms in parallel.
	groupID, err := winter.Group(client, ctx, []winter.Task{
		Build{OS: "linux", Arch: "amd64"},
		Build{OS: "darwin", Arch: "arm64"},
		Build{OS: "windows", Arch: "amd64"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("group created: %s\n", groupID)

	// Chord: build all platforms, then deploy when all are done.
	chordID, err := winter.Chord(client, ctx,
		[]winter.Task{
			Build{OS: "linux", Arch: "amd64"},
			Build{OS: "darwin", Arch: "arm64"},
		},
		Deploy{Version: "1.4.0"},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("chord created: %s\n", chordID)

	// Start server to process everything.
	server, err := winter.NewServer(redisCfg, winter.ServerConfig{
		Concurrency: 10,
		Queues:      winter.Queues("default", 1),
	})
	if err != nil {
		log.Fatal(err)
	}

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[ProcessOrder]) error {
		fmt.Printf("processing order %s\n", job.Args.OrderID)
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[GenerateInvoice]) error {
		fmt.Printf("generating invoice for %s\n", job.Args.OrderID)
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[SendReceipt]) error {
		fmt.Printf("sending receipt for %s\n", job.Args.OrderID)
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[Build]) error {
		fmt.Printf("building %s/%s\n", job.Args.OS, job.Args.Arch)
		return nil
	})

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[Deploy]) error {
		fmt.Printf("deploying %s\n", job.Args.Version)
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
