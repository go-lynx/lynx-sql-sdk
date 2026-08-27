package base

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
)

// HealthReporter is implemented by database plugins to expose a no-reconnect health
// probe for the HealthChecker. Unlike CheckHealth, ReportHealth never triggers
// reconnection — it only marks the connection as unhealthy when a query fails.
type HealthReporter interface {
	ReportHealth() error
	Name() string
}

// HealthChecker performs periodic health checks and updates isHealthy.
// It only reports health state — recovery (Reconnect) is handled exclusively
// by AutoReconnector to avoid redundant concurrent reconnect attempts.
type HealthChecker struct {
	target      HealthReporter
	interval    time.Duration
	customQuery string

	mu           sync.Mutex
	lastCheck    time.Time
	isHealthy    bool
	failureCount int64 // Count of consecutive failures

	stopChan chan struct{}
	stopOnce sync.Once
	stopped  bool
	wg       sync.WaitGroup
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(target HealthReporter, interval time.Duration, customQuery string) *HealthChecker {
	return &HealthChecker{
		target:      target,
		interval:    interval,
		customQuery: customQuery,
		isHealthy:   true,
		stopChan:    make(chan struct{}),
	}
}

// Start starts the health check routine
func (h *HealthChecker) Start(ctx context.Context) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.run(ctx)
	}()
}

// Stop stops the health checker
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	stopped := h.stopped
	h.mu.Unlock()

	if !stopped {
		h.stopOnce.Do(func() {
			close(h.stopChan)
			h.mu.Lock()
			h.stopped = true
			h.mu.Unlock()
		})
	}
	h.wg.Wait()
}

// IsHealthy returns the current health status
func (h *HealthChecker) IsHealthy() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.isHealthy
}

// run performs periodic health checks
func (h *HealthChecker) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic in health-check goroutine for %s: %v", h.target.Name(), r)
		}
	}()
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.performHealthCheck()
		case <-h.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// performHealthCheck performs a single health check and updates isHealthy.
// Recovery is intentionally not attempted here; AutoReconnector polls
// IsConnected() independently and calls Reconnect() when needed.
func (h *HealthChecker) performHealthCheck() {
	err := h.target.ReportHealth()

	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastCheck = time.Now()

	if err != nil {
		h.failureCount++
		if h.isHealthy {
			log.Errorf("Health check failed for %s: %v", h.target.Name(), err)
		}
		h.isHealthy = false
		return
	}

	// Reset failure count on success
	h.failureCount = 0

	// Only log on state transition from unhealthy to healthy to avoid log spam
	if !h.isHealthy {
		log.Infof("Health check recovered for %s", h.target.Name())
	}
	h.isHealthy = true
}
