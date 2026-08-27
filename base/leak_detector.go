package base

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
)

// LeakDetector detects connection leaks by monitoring connection usage
type LeakDetector struct {
	target    Monitorable
	threshold time.Duration
	interval  time.Duration
	mu        sync.Mutex
	stopChan  chan struct{}
	stopOnce  sync.Once
	stopped   bool
	wg        sync.WaitGroup

	// Cumulative wait duration observed at the previous sample; used to compute
	// the per-interval delta instead of comparing the lifetime total.
	lastWaitDuration time.Duration
}

// NewLeakDetector creates a new connection leak detector.
// interval controls how often to check; 0 defaults to 30s.
func NewLeakDetector(target Monitorable, threshold time.Duration, interval time.Duration) *LeakDetector {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &LeakDetector{
		target:    target,
		threshold: threshold,
		interval:  interval,
		stopChan:  make(chan struct{}),
	}
}

// Start starts the leak detection routine
func (l *LeakDetector) Start(ctx context.Context) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.run(ctx)
	}()
}

// Stop stops the leak detector
func (l *LeakDetector) Stop() {
	l.mu.Lock()
	stopped := l.stopped
	l.mu.Unlock()

	if !stopped {
		l.stopOnce.Do(func() {
			close(l.stopChan)
			l.mu.Lock()
			l.stopped = true
			l.mu.Unlock()
		})
	}
	l.wg.Wait()
}

// run performs periodic leak detection
func (l *LeakDetector) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic in leak-detector goroutine for %s: %v", l.target.Name(), r)
		}
	}()
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.detectLeaks()
		case <-l.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// detectLeaks checks for potential connection leaks
func (l *LeakDetector) detectLeaks() {
	for _, msg := range l.evaluate(l.target.GetStats()) {
		log.Warnf("%s", msg)
	}
}

// evaluate inspects a stats sample and returns leak warnings, if any.
// WaitDuration in sql.DBStats is cumulative for the lifetime of the pool, so the
// check uses the delta since the previous sample (per-interval wait duration).
func (l *LeakDetector) evaluate(stats *ConnectionPoolStats) []string {
	if stats == nil {
		return nil
	}

	l.mu.Lock()
	waitDelta := stats.WaitDuration - l.lastWaitDuration
	if waitDelta < 0 {
		// Pool was replaced (e.g. reconnect) and counters reset.
		waitDelta = stats.WaitDuration
	}
	l.lastWaitDuration = stats.WaitDuration
	l.mu.Unlock()

	var warnings []string

	// Check if connections are in use for too long
	// This is a simplified check - in a real implementation, we'd track individual connections
	if stats.InUse > 0 {
		// If connections are in use and pool is near capacity, it might indicate leaks
		if stats.MaxOpenConnections > 0 {
			usage := float64(stats.OpenConnections) / float64(stats.MaxOpenConnections)
			if usage >= 0.9 && stats.InUse == stats.OpenConnections {
				// All connections are in use, potential leak
				warnings = append(warnings, fmt.Sprintf("Potential connection leak detected for %s: all connections (%d/%d) are in use",
					l.target.Name(), stats.OpenConnections, stats.MaxOpenConnections))
			}
		}

		// Check wait duration accumulated during the last interval - long waits might indicate leaks
		if waitDelta > l.threshold {
			warnings = append(warnings, fmt.Sprintf("Long connection wait detected for %s: %v in last interval (threshold: %v). Possible connection leak.",
				l.target.Name(), waitDelta, l.threshold))
		}
	}
	return warnings
}
