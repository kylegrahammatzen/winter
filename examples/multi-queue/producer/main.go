// Producer example: enqueues tasks to different queues based on task options.
//
// Run with: go run ./examples/multi-queue/producer
// Requires Redis running on localhost:6379.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kylegrahammatzen/winter"
	"github.com/kylegrahammatzen/winter/examples/multi-queue/tasks"
)

func main() {
	client, err := winter.NewClient(winter.RedisConfig{Addr: "localhost:6379"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Goes to "payments" queue (from TaskOptions).
	job1, err := winter.Enqueue(client, ctx, tasks.ProcessPayment{
		PaymentID: "pay-456",
		Amount:    2999,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued %s to payments queue\n", job1.ID)

	// Goes to "default" queue.
	job2, err := winter.Enqueue(client, ctx, tasks.SendNotification{
		UserID:  42,
		Message: "Your order has shipped",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued %s to default queue\n", job2.ID)

	// Goes to "low" queue (from TaskOptions).
	job3, err := winter.Enqueue(client, ctx, tasks.GenerateReport{
		ReportType: "monthly-summary",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued %s to low queue\n", job3.ID)
}
