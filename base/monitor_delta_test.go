package base

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-lynx/lynx-sql-sdk/interfaces"
)

// fakeStatsProvider is a Monitorable whose stats can be swapped between samples.
type fakeStatsProvider struct {
	mu    sync.Mutex
	stats *ConnectionPoolStats
}

func (f *fakeStatsProvider) GetStats() *ConnectionPoolStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := *f.stats
	return &s
}

func (f *fakeStatsProvider) Name() string { return "fake" }

func (f *fakeStatsProvider) set(stats *ConnectionPoolStats) {
	f.mu.Lock()
	f.stats = stats
	f.mu.Unlock()
}

func TestPoolMonitor_WaitThresholdsUseDeltaNotCumulative(t *testing.T) {
	provider := &fakeStatsProvider{stats: &ConnectionPoolStats{MaxOpenConnections: 100, OpenConnections: 1}}
	m := NewPoolMonitor(provider, time.Second, &PoolThresholds{
		UsagePercentage: 0.8,
		WaitDuration:    5 * time.Second,
		WaitCount:       10,
	})

	// First sample: lifetime totals far above threshold -> alerts (delta from zero baseline).
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 100, OpenConnections: 1, WaitCount: 50, WaitDuration: 300 * time.Second})
	alerts, severity, wc, wd := m.evaluate(provider.GetStats())
	if len(alerts) != 2 || severity != "critical" || wc != 50 || wd != 300*time.Second {
		t.Fatalf("first sample: alerts=%v severity=%s wc=%d wd=%v", alerts, severity, wc, wd)
	}

	// Second sample: totals unchanged -> no wait in this interval -> no alert.
	alerts, _, wc, wd = m.evaluate(provider.GetStats())
	if len(alerts) != 0 || wc != 0 || wd != 0 {
		t.Fatalf("unchanged totals must not alert: alerts=%v wc=%d wd=%v", alerts, wc, wd)
	}

	// Third sample: small increment below thresholds -> still no alert even though totals are huge.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 100, OpenConnections: 1, WaitCount: 52, WaitDuration: 301 * time.Second})
	alerts, _, wc, wd = m.evaluate(provider.GetStats())
	if len(alerts) != 0 || wc != 2 || wd != time.Second {
		t.Fatalf("small delta must not alert: alerts=%v wc=%d wd=%v", alerts, wc, wd)
	}

	// Fourth sample: increment above thresholds -> alert with per-interval values.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 100, OpenConnections: 1, WaitCount: 64, WaitDuration: 307 * time.Second})
	alerts, severity, wc, wd = m.evaluate(provider.GetStats())
	if len(alerts) != 2 || severity != "warning" || wc != 12 || wd != 6*time.Second {
		t.Fatalf("large delta must alert: alerts=%v severity=%s wc=%d wd=%v", alerts, severity, wc, wd)
	}

	// Counter reset (pool replaced): current values are used as the delta.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 100, OpenConnections: 1, WaitCount: 1, WaitDuration: time.Second})
	alerts, _, wc, wd = m.evaluate(provider.GetStats())
	if len(alerts) != 0 || wc != 1 || wd != time.Second {
		t.Fatalf("after reset: alerts=%v wc=%d wd=%v", alerts, wc, wd)
	}
}

func TestLeakDetector_WaitThresholdUsesDeltaNotCumulative(t *testing.T) {
	provider := &fakeStatsProvider{stats: &ConnectionPoolStats{}}
	l := NewLeakDetector(provider, 300*time.Second, 0)

	// Lifetime total above threshold on first sample -> warning.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 10, OpenConnections: 2, InUse: 1, WaitDuration: 400 * time.Second})
	if w := l.evaluate(provider.GetStats()); len(w) != 1 {
		t.Fatalf("expected one warning on first sample, got %v", w)
	}

	// Same total again -> no new wait in this interval -> no warning (previously alerted forever).
	if w := l.evaluate(provider.GetStats()); len(w) != 0 {
		t.Fatalf("unchanged total must not warn, got %v", w)
	}

	// Small increment -> no warning.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 10, OpenConnections: 2, InUse: 1, WaitDuration: 410 * time.Second})
	if w := l.evaluate(provider.GetStats()); len(w) != 0 {
		t.Fatalf("small delta must not warn, got %v", w)
	}

	// Increment above threshold -> warning.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 10, OpenConnections: 2, InUse: 1, WaitDuration: 720 * time.Second})
	if w := l.evaluate(provider.GetStats()); len(w) != 1 {
		t.Fatalf("large delta must warn, got %v", w)
	}

	// Counter reset after pool replacement -> uses current value.
	provider.set(&ConnectionPoolStats{MaxOpenConnections: 10, OpenConnections: 2, InUse: 1, WaitDuration: time.Second})
	if w := l.evaluate(provider.GetStats()); len(w) != 0 {
		t.Fatalf("after reset must not warn, got %v", w)
	}
}

// fakeHealthTarget fails health checks and counts reconnect attempts made by the checker.
type fakeHealthTarget struct {
	checks     atomic.Int64
	reconnects atomic.Int64
}

func (f *fakeHealthTarget) ReportHealth() error {
	f.checks.Add(1)
	return errors.New("unhealthy")
}
func (f *fakeHealthTarget) Name() string      { return "fake" }
func (f *fakeHealthTarget) Reconnect() error  { f.reconnects.Add(1); return nil }
func (f *fakeHealthTarget) IsConnected() bool { return false }

func TestHealthChecker_DoesNotReconnectOnItsOwn(t *testing.T) {
	target := &fakeHealthTarget{}
	h := NewHealthChecker(target, time.Hour, "")

	h.performHealthCheck()

	if target.checks.Load() != 1 {
		t.Fatalf("expected one health check, got %d", target.checks.Load())
	}
	if target.reconnects.Load() != 0 {
		t.Fatalf("health checker must not issue its own reconnect (AutoReconnector handles recovery), got %d", target.reconnects.Load())
	}
	if h.IsHealthy() {
		t.Fatal("checker should report unhealthy after failed check")
	}
}

func TestBackgroundComponents_StopWaitsForGoroutine(t *testing.T) {
	provider := &fakeStatsProvider{stats: &ConnectionPoolStats{}}
	target := &fakeHealthTarget{}
	ctx := context.Background()

	pm := NewPoolMonitor(provider, time.Millisecond, nil)
	ld := NewLeakDetector(provider, time.Second, 0)
	hc := NewHealthChecker(target, time.Millisecond, "")
	ar := NewAutoReconnector(target, time.Millisecond, 0)

	pm.Start(ctx)
	ld.Start(ctx)
	hc.Start(ctx)
	ar.Start(ctx)

	time.Sleep(5 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		pm.Stop()
		ld.Stop()
		hc.Stop()
		ar.Stop()
		// Stop must be idempotent and still not block.
		pm.Stop()
		ld.Stop()
		hc.Stop()
		ar.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after goroutines exited")
	}

	// After Stop returned, the goroutines must have exited: no further checks are observed.
	checks := target.checks.Load()
	time.Sleep(5 * time.Millisecond)
	if got := target.checks.Load(); got != checks {
		t.Fatalf("health checker still running after Stop: %d -> %d", checks, got)
	}
}

func TestSQLPlugin_CleanupTasksWaitsForBackgroundGoroutines(t *testing.T) {
	config := &interfaces.Config{
		Driver:                 successDriverName,
		DSN:                    "success",
		MaxOpenConns:           10,
		MaxIdleConns:           5,
		HealthCheckInterval:    1,
		MonitorEnabled:         true,
		MonitorInterval:        1,
		LeakDetectionEnabled:   true,
		LeakDetectionThreshold: 1,
		AutoReconnectInterval:  1,
		WarmupEnabled:          true,
		WarmupConns:            1,
	}
	plugin := NewBaseSQLPlugin("test-id", "test-plugin", "Test plugin", "v1.0.0", "test.prefix", 100, config)
	rt := &mockRuntime{config: map[string]any{"test.prefix": config}}
	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}
	if err := plugin.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- plugin.CleanupTasks() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CleanupTasks failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CleanupTasks did not return")
	}

	if plugin.IsConnected() {
		t.Fatal("plugin should be disconnected after cleanup")
	}
}

func TestSQLPlugin_GetDBDoesNotReconnectWhenAutoReconnectDisabled(t *testing.T) {
	autoReconnect := false
	config := &interfaces.Config{
		Driver:                successDriverName,
		DSN:                   "success",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		AutoReconnectEnabled:  &autoReconnect,
		AutoReconnectInterval: 5,
	}
	plugin := NewBaseSQLPlugin("test-id", "test-plugin", "Test plugin", "v1.0.0", "test.prefix", 100, config)
	rt := &mockRuntime{config: map[string]any{"test.prefix": config}}
	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}
	if err := plugin.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}
	t.Cleanup(func() { _ = plugin.CleanupTasks() })

	plugin.mu.RLock()
	db := plugin.db
	plugin.mu.RUnlock()
	_ = db.Close()
	plugin.connected.Store(false)

	if _, err := plugin.GetDB(); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected without reconnect, got %v", err)
	}
	plugin.mu.RLock()
	same := plugin.db == db
	plugin.mu.RUnlock()
	if !same {
		t.Fatal("GetDB must not replace the pool when auto-reconnect is disabled")
	}
}
