// Package config parses Winter's YAML configuration file into strongly typed
// structs used by the server and CLI.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for a Winter server.
type Config struct {
	Redis     RedisConfig            `yaml:"redis"`
	Server    ServerConfig           `yaml:"server"`
	Queues    map[string]QueueConfig `yaml:"queues"`
	Workers   WorkerConfig           `yaml:"workers"`
	Scheduler SchedulerConfig        `yaml:"scheduler"`
	Cron      []CronConfig           `yaml:"cron"`
}

// RedisConfig holds connection parameters for Redis.
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// ServerConfig controls the gRPC and admin ports plus log level.
type ServerConfig struct {
	GRPCPort string `yaml:"grpc_port"`
	LogLevel string `yaml:"log_level"`
}

// QueueConfig holds per-queue defaults.
type QueueConfig struct {
	MaxRetries      int    `yaml:"max_retries"`
	Backoff         string `yaml:"backoff"`
	BackoffBase     string `yaml:"backoff_base"`
	PriorityDefault int    `yaml:"priority_default"`
}

// WorkerConfig controls heartbeat and recovery timing.
type WorkerConfig struct {
	HeartbeatInterval string `yaml:"heartbeat_interval"`
	StaleThreshold    string `yaml:"stale_threshold"`
	RecoveryInterval  string `yaml:"recovery_interval"`
	Concurrency       int    `yaml:"concurrency"`
}

// SchedulerConfig controls the delayed job promotion interval.
type SchedulerConfig struct {
	PollInterval string `yaml:"poll_interval"`
}

// CronConfig defines a periodic job entry.
type CronConfig struct {
	Name     string `yaml:"name"`
	Schedule string `yaml:"schedule"`
	Queue    string `yaml:"queue"`
	Kind     string `yaml:"kind"`
	Payload  string `yaml:"payload"`
}

// Load reads and parses a YAML config file from the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("winter: read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("winter: parse config: %w", err)
	}

	applyDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Redis.Addr == "" {
		cfg.Redis.Addr = "localhost:6379"
	}
	if cfg.Server.GRPCPort == "" {
		cfg.Server.GRPCPort = "50051"
	}
	if cfg.Server.LogLevel == "" {
		cfg.Server.LogLevel = "info"
	}
	if cfg.Workers.Concurrency <= 0 {
		cfg.Workers.Concurrency = 10
	}
	if cfg.Workers.HeartbeatInterval == "" {
		cfg.Workers.HeartbeatInterval = "15s"
	}
	if cfg.Workers.StaleThreshold == "" {
		cfg.Workers.StaleThreshold = "60s"
	}
	if cfg.Workers.RecoveryInterval == "" {
		cfg.Workers.RecoveryInterval = "30s"
	}
	if cfg.Scheduler.PollInterval == "" {
		cfg.Scheduler.PollInterval = "500ms"
	}
}

func validate(cfg *Config) error {
	if _, err := time.ParseDuration(cfg.Workers.HeartbeatInterval); err != nil {
		return fmt.Errorf("winter: invalid heartbeat_interval %q: %w", cfg.Workers.HeartbeatInterval, err)
	}
	if _, err := time.ParseDuration(cfg.Workers.StaleThreshold); err != nil {
		return fmt.Errorf("winter: invalid stale_threshold %q: %w", cfg.Workers.StaleThreshold, err)
	}
	if _, err := time.ParseDuration(cfg.Workers.RecoveryInterval); err != nil {
		return fmt.Errorf("winter: invalid recovery_interval %q: %w", cfg.Workers.RecoveryInterval, err)
	}
	if _, err := time.ParseDuration(cfg.Scheduler.PollInterval); err != nil {
		return fmt.Errorf("winter: invalid poll_interval %q: %w", cfg.Scheduler.PollInterval, err)
	}

	for name, qcfg := range cfg.Queues {
		if qcfg.BackoffBase != "" {
			if _, err := time.ParseDuration(qcfg.BackoffBase); err != nil {
				return fmt.Errorf("winter: invalid backoff_base for queue %q: %w", name, err)
			}
		}
	}

	for _, c := range cfg.Cron {
		if c.Name == "" {
			return fmt.Errorf("winter: cron entry missing name")
		}
		if c.Schedule == "" {
			return fmt.Errorf("winter: cron entry %q missing schedule", c.Name)
		}
		if c.Kind == "" {
			return fmt.Errorf("winter: cron entry %q missing kind", c.Name)
		}
	}

	return nil
}

// ParseDuration is a convenience wrapper around time.ParseDuration that returns
// a fallback when the input is empty.
func ParseDuration(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}
