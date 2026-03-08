package winter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidTransition(t *testing.T) {
	tests := []struct {
		name  string
		from  JobState
		to    JobState
		valid bool
	}{
		{"pending to active", StatePending, StateActive, true},
		{"active to completed", StateActive, StateCompleted, true},
		{"active to failed", StateActive, StateFailed, true},
		{"active to retry", StateActive, StateRetry, true},
		{"active to cancelled", StateActive, StateCancelled, true},
		{"retry to pending", StateRetry, StatePending, true},
		{"retry to active", StateRetry, StateActive, true},
		{"failed to dead", StateFailed, StateDead, true},
		{"failed to pending", StateFailed, StatePending, true},
		{"dead to pending", StateDead, StatePending, true},

		{"pending to completed", StatePending, StateCompleted, false},
		{"pending to dead", StatePending, StateDead, false},
		{"completed to active", StateCompleted, StateActive, false},
		{"completed to pending", StateCompleted, StatePending, false},
		{"cancelled to pending", StateCancelled, StatePending, false},
		{"dead to active", StateDead, StateActive, false},
		{"active to pending", StateActive, StatePending, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.valid, ValidTransition(tt.from, tt.to))
		})
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := Exponential(time.Second)

	assert.Equal(t, time.Second, b.Next(0))
	assert.Equal(t, 2*time.Second, b.Next(1))
	assert.Equal(t, 4*time.Second, b.Next(2))
	assert.Equal(t, 8*time.Second, b.Next(3))
}

func TestLinearBackoff(t *testing.T) {
	b := Linear(5 * time.Second)

	assert.Equal(t, 5*time.Second, b.Next(0))
	assert.Equal(t, 10*time.Second, b.Next(1))
	assert.Equal(t, 15*time.Second, b.Next(2))
}

func TestFixedBackoff(t *testing.T) {
	b := Fixed(3 * time.Second)

	assert.Equal(t, 3*time.Second, b.Next(0))
	assert.Equal(t, 3*time.Second, b.Next(1))
	assert.Equal(t, 3*time.Second, b.Next(5))
}

func TestSentinelErrors(t *testing.T) {
	t.Run("reschedule", func(t *testing.T) {
		err := Reschedule(30 * time.Second)
		require.Error(t, err)

		delay, ok := IsReschedule(err)
		require.True(t, ok)
		assert.Equal(t, 30*time.Second, delay)

		_, ok = IsReschedule(assert.AnError)
		assert.False(t, ok)
	})

	t.Run("cancel", func(t *testing.T) {
		err := Cancel("resource deleted")
		require.Error(t, err)

		reason, ok := IsCancel(err)
		require.True(t, ok)
		assert.Equal(t, "resource deleted", reason)

		_, ok = IsCancel(assert.AnError)
		assert.False(t, ok)
	})

	t.Run("skip retry", func(t *testing.T) {
		assert.ErrorIs(t, ErrSkipRetry, ErrSkipRetry)
	})
}
