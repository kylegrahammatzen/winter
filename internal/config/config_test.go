package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "winter.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}

// Parses a full config file and verifies all fields are populated.
func TestLoadFullConfig(t *testing.T) {
	path := writeConfig(t, `
redis:
  addr: "redis.example.com:6380"
  password: "secret"
  db: 2

server:
  grpc_port: "9090"
  log_level: "debug"

queues:
  emails:
    max_retries: 5
    backoff: "exponential"
    backoff_base: "1s"
    priority_default: 5
  payments:
    max_retries: 10
    backoff: "linear"
    backoff_base: "500ms"
    priority_default: 0

workers:
  heartbeat_interval: "10s"
  stale_threshold: "45s"
  recovery_interval: "20s"
  concurrency: 50

scheduler:
  poll_interval: "1s"

cron:
  - name: "daily-cleanup"
    schedule: "0 2 * * *"
    queue: "maintenance"
    kind: "cleanup.run"
    payload: '{"task":"cleanup"}'
`)

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.Equal(t, "redis.example.com:6380", cfg.Redis.Addr)
	assert.Equal(t, "secret", cfg.Redis.Password)
	assert.Equal(t, 2, cfg.Redis.DB)
	assert.Equal(t, "9090", cfg.Server.GRPCPort)
	assert.Equal(t, "debug", cfg.Server.LogLevel)
	assert.Equal(t, 5, cfg.Queues["emails"].MaxRetries)
	assert.Equal(t, "exponential", cfg.Queues["emails"].Backoff)
	assert.Equal(t, 10, cfg.Queues["payments"].MaxRetries)
	assert.Equal(t, 50, cfg.Workers.Concurrency)
	assert.Equal(t, "10s", cfg.Workers.HeartbeatInterval)
	assert.Equal(t, "1s", cfg.Scheduler.PollInterval)
	require.Len(t, cfg.Cron, 1)
	assert.Equal(t, "daily-cleanup", cfg.Cron[0].Name)
	assert.Equal(t, "0 2 * * *", cfg.Cron[0].Schedule)
}

// An empty config file uses sensible defaults for all fields.
func TestLoadDefaults(t *testing.T) {
	path := writeConfig(t, "")

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.Equal(t, "localhost:6379", cfg.Redis.Addr)
	assert.Equal(t, "50051", cfg.Server.GRPCPort)
	assert.Equal(t, "info", cfg.Server.LogLevel)
	assert.Equal(t, 10, cfg.Workers.Concurrency)
	assert.Equal(t, "15s", cfg.Workers.HeartbeatInterval)
	assert.Equal(t, "60s", cfg.Workers.StaleThreshold)
	assert.Equal(t, "30s", cfg.Workers.RecoveryInterval)
	assert.Equal(t, "500ms", cfg.Scheduler.PollInterval)
}

// An invalid duration string is rejected during validation.
func TestLoadInvalidDuration(t *testing.T) {
	path := writeConfig(t, `
workers:
  heartbeat_interval: "not-a-duration"
`)

	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid heartbeat_interval")
}

// A cron entry without a name is rejected.
func TestLoadCronMissingName(t *testing.T) {
	path := writeConfig(t, `
cron:
  - schedule: "* * * * *"
    kind: "test.job"
`)

	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing name")
}

// A cron entry without a schedule is rejected.
func TestLoadCronMissingSchedule(t *testing.T) {
	path := writeConfig(t, `
cron:
  - name: "test"
    kind: "test.job"
`)

	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing schedule")
}

// A cron entry without a kind is rejected.
func TestLoadCronMissingKind(t *testing.T) {
	path := writeConfig(t, `
cron:
  - name: "test"
    schedule: "* * * * *"
`)

	_, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing kind")
}

// A missing config file returns an error.
func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

// ParseDuration returns the parsed value or the fallback.
func TestParseDuration(t *testing.T) {
	assert.Equal(t, 5*ParseDuration("1s", 0), 5*ParseDuration("1s", 0))
	assert.Equal(t, ParseDuration("", 42), ParseDuration("", 42))
	assert.Equal(t, ParseDuration("garbage", 99), ParseDuration("garbage", 99))
}
