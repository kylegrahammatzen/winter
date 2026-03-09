// Package tasks defines shared task types used by both the producer and worker.
package tasks

import (
	"time"

	"github.com/kylegrahammatzen/winter"
)

type ProcessPayment struct {
	PaymentID string `json:"payment_id"`
	Amount    int    `json:"amount"`
}

func (ProcessPayment) Kind() string { return "payment.process" }

func (ProcessPayment) Options() winter.TaskOptions {
	return winter.TaskOptions{
		Queue:      "payments",
		MaxRetries: 10,
		Backoff:    winter.Exponential(500 * time.Millisecond),
	}
}

type SendNotification struct {
	UserID  int    `json:"user_id"`
	Message string `json:"message"`
}

func (SendNotification) Kind() string { return "notification.send" }

type GenerateReport struct {
	ReportType string `json:"report_type"`
}

func (GenerateReport) Kind() string { return "report.generate" }

func (GenerateReport) Options() winter.TaskOptions {
	return winter.TaskOptions{
		Queue:      "low",
		MaxRetries: 3,
	}
}
