// Priority example: demonstrates that lower priority values are dequeued first.
//
// Run with: go run ./examples/priority
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

type Task struct {
	Name string `json:"name"`
}

func (Task) Kind() string { return "priority.task" }

func main() {
	redisCfg := winter.RedisConfig{Addr: "localhost:6379"}

	client, err := winter.NewClient(redisCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue tasks with different priorities.
	// Lower values = higher priority = dequeued first.
	tasks := []struct {
		name     string
		priority int
	}{
		{"low priority task", 10},
		{"critical task", 0},
		{"normal task", 5},
		{"high priority task", 1},
		{"background task", 20},
	}

	for _, t := range tasks {
		job, err := winter.Enqueue(client, ctx, Task{Name: t.name}, winter.Priority(t.priority))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("enqueued %s (priority=%d, id=%s)\n", t.name, t.priority, job.ID)
	}

	server, err := winter.NewServer(redisCfg, winter.ServerConfig{
		Concurrency: 1,
		Queues:      winter.Queues("default", 1),
	})
	if err != nil {
		log.Fatal(err)
	}

	winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[Task]) error {
		fmt.Printf("processing: %s (priority=%d)\n", job.Args.Name, job.Priority)
		return nil
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		<-sig
		server.Stop()
	}()

	fmt.Println("\nserver starting (concurrency=1 to show ordering), press ctrl+c to stop")
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
