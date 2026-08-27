package base

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
)

// PoolMonitor monitors connection pool health and triggers alerts
type PoolMonitor struct {
	target        Monitorable
	interval      time.Duration
	thresholds    *PoolThresholds
	mu            sync.Mutex
	lastAlert     time.Time
	alertCooldown time.Duration
	stopChan      chan struct{}
	stopOnce      sync.Once
	stopped       bool
	wg            sync.WaitGroup
	// Track alert severity to adjust cooldown
	lastSeverity string
	// Cumulative wait stats observed at the previous sample; sql.DBStats counters
	// are lifetime totals, so thresholds are applied to the per-interval delta.
	lastWaitCount    int64
	lastWaitDuration time.Duration
}

// PoolThresholds defines alert thresholds for connection pool monitoring
type PoolThresholds struct {
	UsagePercentage float64       // Alert when pool usage exceeds this (0.0-1.0)
	WaitDuration    time.Duration // Alert when wait duration accumulated in one interval exceeds this
	WaitCount       int64         // Alert when wait count accumulated in one interval exceeds this
	AlertCooldown   time.Duration // Minimum time between alerts; 0 = default (60s)
}

// Monitorable interface for components that can be monitored
type Monitorable interface {
	GetStats() *ConnectionPoolStats
	Name() string
}

// NewPoolMonitor creates a new connection pool monitor
func NewPoolMonitor(target Monitorable, interval time.Duration, thresholds *PoolThresholds) *PoolMonitor {
	if thresholds == nil {
		thresholds = &PoolThresholds{
			UsagePercentage: 0.8,             // 80%
			WaitDuration:    5 * time.Second, // 5 seconds
			WaitCount:       10,              // 10 waits
		}
	}

	cooldown := 60 * time.Second
	if thresholds.AlertCooldown > 0 {
		cooldown = thresholds.AlertCooldown
	}
	return &PoolMonitor{
		target:        target,
		interval:      interval,
		thresholds:    thresholds,
		alertCooldown: cooldown,
		stopChan:      make(chan struct{}),
	}
}

// Start starts the monitoring routine
func (m *PoolMonitor) Start(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run(ctx)
	}()
}

// Stop stops the monitor
func (m *PoolMonitor) Stop() {
	m.mu.Lock()
	stopped := m.stopped
	m.mu.Unlock()

	if !stopped {
		m.stopOnce.Do(func() {
			close(m.stopChan)
			m.mu.Lock()
			m.stopped = true
			m.mu.Unlock()
		})
	}
	m.wg.Wait()
}

// run performs periodic monitoring
func (m *PoolMonitor) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic in pool-monitor goroutine for %s: %v", m.target.Name(), r)
		}
	}()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkAndAlert()
		case <-m.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// checkAndAlert checks pool stats and triggers alerts if thresholds are exceeded
func (m *PoolMonitor) checkAndAlert() {
	stats := m.target.GetStats()
	alerts, severity, waitCount, waitDuration := m.evaluate(stats)

	// Log alerts if any
	if len(alerts) > 0 {
		m.mu.Lock()
		// Adjust cooldown based on severity: critical alerts have shorter cooldown
		cooldown := m.alertCooldown
		if severity == "critical" && m.lastSeverity != "critical" {
			cooldown = 30 * time.Second // Shorter cooldown for critical alerts
		}
		shouldAlert := time.Since(m.lastAlert) > cooldown
		if shouldAlert {
			m.lastAlert = time.Now()
			m.lastSeverity = severity
		}
		m.mu.Unlock()

		if shouldAlert {
			log.Warnf("Connection pool alert [%s] for %s: %v (Open=%d/%d, InUse=%d, Idle=%d, WaitCount=%d, WaitDuration=%v in last interval)",
				severity,
				m.target.Name(),
				alerts,
				stats.OpenConnections,
				stats.MaxOpenConnections,
				stats.InUse,
				stats.Idle,
				waitCount,
				waitDuration,
			)
		}
	} else {
		// Reset severity if no alerts
		m.mu.Lock()
		if m.lastSeverity != "" {
			m.lastSeverity = ""
		}
		m.mu.Unlock()
	}
}

// evaluate applies the thresholds to a stats sample and returns the triggered alerts,
// the severity, and the per-interval wait count/duration used for the wait checks.
// WaitCount/WaitDuration are cumulative in sql.DBStats, so the delta since the
// previous sample is used; otherwise the alert would fire forever once the
// lifetime total exceeded the threshold.
func (m *PoolMonitor) evaluate(stats *ConnectionPoolStats) (alerts []string, severity string, waitCount int64, waitDuration time.Duration) {
	severity = "warning"
	if stats == nil {
		return nil, severity, 0, 0
	}

	m.mu.Lock()
	waitCount = stats.WaitCount - m.lastWaitCount
	waitDuration = stats.WaitDuration - m.lastWaitDuration
	if waitCount < 0 || waitDuration < 0 {
		// Pool was replaced (e.g. reconnect) and counters reset.
		waitCount = stats.WaitCount
		waitDuration = stats.WaitDuration
	}
	m.lastWaitCount = stats.WaitCount
	m.lastWaitDuration = stats.WaitDuration
	m.mu.Unlock()

	// Check pool usage percentage
	if stats.MaxOpenConnections > 0 {
		usage := float64(stats.OpenConnections) / float64(stats.MaxOpenConnections)
		if usage >= m.thresholds.UsagePercentage {
			alerts = append(alerts, "high pool usage")
			if usage >= 0.95 {
				severity = "critical"
			}
		}
	}

	// Check wait duration accumulated during the last interval
	if waitDuration > m.thresholds.WaitDuration {
		alerts = append(alerts, "high wait duration")
		if waitDuration > m.thresholds.WaitDuration*2 {
			severity = "critical"
		}
	}

	// Check wait count accumulated during the last interval
	if waitCount > m.thresholds.WaitCount {
		alerts = append(alerts, "high wait count")
		if waitCount > m.thresholds.WaitCount*5 {
			severity = "critical"
		}
	}

	return alerts, severity, waitCount, waitDuration
}
