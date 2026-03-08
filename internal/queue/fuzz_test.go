package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzJobRecordRoundTrip(f *testing.F) {
	f.Add("job-1", "email.send", "default", 5, "pending", []byte(`{"to":"test@example.com"}`), 0, 3, int64(1700000000000), int64(0), int64(0), int64(0), "")

	f.Fuzz(func(t *testing.T, id, kind, queue string, priority int, state string, payload []byte, attempt, maxRetries int, createdAt, scheduledAt, startedAt, completedAt int64, lastError string) {
		if id == "" || kind == "" || queue == "" {
			t.Skip()
		}

		original := &JobRecord{
			ID:          id,
			Kind:        kind,
			Queue:       queue,
			Priority:    priority,
			State:       state,
			Payload:     payload,
			Attempt:     attempt,
			MaxRetries:  maxRetries,
			CreatedAt:   createdAt,
			ScheduledAt: scheduledAt,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			LastError:   lastError,
		}

		m := map[string]string{
			"id":           original.ID,
			"kind":         original.Kind,
			"queue":        original.Queue,
			"priority":     itoa(original.Priority),
			"state":        original.State,
			"payload":      string(original.Payload),
			"attempt":      itoa(original.Attempt),
			"max_retries":  itoa(original.MaxRetries),
			"created_at":   i64toa(original.CreatedAt),
			"scheduled_at": i64toa(original.ScheduledAt),
			"started_at":   i64toa(original.StartedAt),
			"completed_at": i64toa(original.CompletedAt),
			"last_error":   original.LastError,
		}

		roundTripped, err := parseJobRecordFromMap(m)
		require.NoError(t, err)

		require.Equal(t, original.ID, roundTripped.ID)
		require.Equal(t, original.Kind, roundTripped.Kind)
		require.Equal(t, original.Queue, roundTripped.Queue)
		require.Equal(t, original.Priority, roundTripped.Priority)
		require.Equal(t, original.State, roundTripped.State)
		require.Equal(t, original.Payload, roundTripped.Payload)
		require.Equal(t, original.Attempt, roundTripped.Attempt)
		require.Equal(t, original.MaxRetries, roundTripped.MaxRetries)
		require.Equal(t, original.CreatedAt, roundTripped.CreatedAt)
		require.Equal(t, original.ScheduledAt, roundTripped.ScheduledAt)
		require.Equal(t, original.StartedAt, roundTripped.StartedAt)
		require.Equal(t, original.CompletedAt, roundTripped.CompletedAt)
		require.Equal(t, original.LastError, roundTripped.LastError)
	})
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

func i64toa(i int64) string {
	return fmt.Sprintf("%d", i)
}
