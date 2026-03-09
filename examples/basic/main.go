// Basic example: define a task, enqueue it, and process it.
//
// Run with: go run ./examples/basic
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

type SendEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func (SendEmail) Kind() string { return "email.send" }

func main() {
	redisCfg := winter.RedisConfig{Addr: "localhost:6379"}

	client, err := winter.NewClient(redisCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	job, err := winter.Enqueue(client, ctx, SendEmail{
		To:      "user@example.com",
		Subject: "Welcome to Winter",
		Body:    "Thanks for trying out Winter!",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued job %s\n", job.ID)

	// Enqueue a delayed job.
	delayed, err := winter.Enqueue(client, ctx, SendEmail{
		To:      "user@example.com",
		Subject: "Follow up",
		Body:    "How's it going?",
	}, winter.In(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued delayed job %s (runs in 10s)\n", delayed.ID)

	server, err := winter.NewServer(redisCfg, winter.ServerConfig{
		Concurrency: 5,
		Queues:      winter.Queues("default", 1),
	})
	if err != nil {
		log.Fatal(err)
	}

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[SendEmail]) error {
		fmt.Printf("sending email to=%s subject=%q\n", job.Args.To, job.Args.Subject)
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
