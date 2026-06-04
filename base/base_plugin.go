// Package base provides the production-grade SQL plugin implementation shared by
// all lynx database plugins (MySQL, PostgreSQL, etc.).  It wraps database/sql with
// a connection pool, health checking, auto-reconnect, pool monitoring, slow-query
// detection, connection-leak detection, and a MetricsRecorder interface that
// individual plugins implement to expose Prometheus metrics.
package base

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx-sql-sdk/interfaces"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
)

var (
	ErrNotConnected  = errors.New("database not connected")
	ErrAlreadyClosed = errors.New("database already closed")
)

const (
	connectionValidationCacheWindow   = 5 * time.Second
	minPoolMaintainDefaultInterval    = 10 * time.Second
	minPoolMaintainMinInterval        = 1 * time.Second
	minPoolMaintainMaxInterval        = 30 * time.Second
	minPoolConnectionWarmupTimeout    = 15 * time.Second
	minPoolSingleConnectionPingWindow = 2 * time.Second
)

// ConnectionPoolStats represents database connection pool statistics
type ConnectionPoolStats struct {
	MaxOpenConnections int64         // Maximum number of open connections
	OpenConnections    int64         // Number of established connections
	InUse              int64         // Number of connections currently in use
	Idle               int64         // Number of idle connections
	MaxIdleConnections int64         // Maximum number of idle connections
	WaitCount          int64         // Total number of connections waited for
	WaitDuration       time.Duration // Total time blocked waiting for a new connection
	MaxIdleClosed      int64         // Total number of connections closed due to SetMaxIdleConns
	MaxLifetimeClosed  int64         // Total number of connections closed due to SetConnMaxLifetime
}

// SQLPlugin provides common functionality for all SQL plugins
type SQLPlugin struct {
	*plugins.BasePlugin

	// Configuration
	config     *interfaces.Config
	confPrefix string
	runtime    plugins.Runtime
	provider   interfaces.DBProvider

	// Database connection
	db      *sql.DB
	dialect string

	// Connection state
	mu          sync.RWMutex
	lifecycleMu sync.Mutex
	connected   atomic.Bool
	closing     atomic.Bool

	// Health check
	healthChecker *HealthChecker

	// Pool monitoring
	poolMonitor *PoolMonitor

	// Auto-reconnect
	autoReconnect *AutoReconnector

	// Connection leak detector
	leakDetector *LeakDetector

	// Query monitor for slow query detection
	queryMonitor *QueryMonitor

	// Metrics recording
	metricsRecorder MetricsRecorder

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// Last successful ping time for connection validation
	lastPingTime atomic.Int64
}

// NewBaseSQLPlugin creates a new base SQL plugin
func NewBaseSQLPlugin(
	id, name, desc, version, confPrefix string,
	weight int,
	config *interfaces.Config,
) *SQLPlugin {
	ctx, cancel := context.WithCancel(context.Background())

	return &SQLPlugin{
		BasePlugin:      plugins.NewBasePlugin(id, name, desc, version, confPrefix, weight),
		config:          config,
		confPrefix:      confPrefix,
		metricsRecorder: &NoOpMetricsRecorder{}, // Default to no-op metrics
		ctx:             ctx,
		cancel:          cancel,
	}
}

// InitializeResources initializes plugin resources
func (p *SQLPlugin) InitializeResources(rt plugins.Runtime) error {
	p.bindRuntime(rt)

	// Load configuration
	if err := rt.GetConfig().Value(p.confPrefix).Scan(p.config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Set default values
	if p.config.MaxOpenConns == 0 {
		p.config.MaxOpenConns = 25
	}
	if p.config.MaxIdleConns == 0 {
		p.config.MaxIdleConns = 5
	}

	// Set default retry values
	if p.config.RetryMaxAttempts == 0 {
		p.config.RetryMaxAttempts = 3
	}
	if p.config.RetryInitialDelay == 0 {
		p.config.RetryInitialDelay = 1
	}
	if p.config.RetryMaxDelay == 0 {
		p.config.RetryMaxDelay = 30
	}
	if p.config.RetryMultiplier == 0 {
		p.config.RetryMultiplier = 2.0
	}

	// Set default monitoring values
	if p.config.MonitorInterval == 0 {
		p.config.MonitorInterval = 30
	}
	if p.config.AlertThresholdUsage == 0 {
		p.config.AlertThresholdUsage = 0.8
	}
	if p.config.AlertThresholdWait == 0 {
		p.config.AlertThresholdWait = 5
	}
	if p.config.AlertThresholdWaitCount == 0 {
		p.config.AlertThresholdWaitCount = 10
	}

	// Set default auto-reconnect values
	// Auto-reconnect is enabled by default for production readiness
	// Default interval to 5 seconds if not set
	// User can disable by setting auto_reconnect_enabled: false
	if p.config.AutoReconnectInterval == 0 {
		p.config.AutoReconnectInterval = 5 // Default 5 seconds
	}
	// 0 means unlimited attempts for max_attempts

	// Set default warmup values
	if p.config.WarmupConns == 0 {
		p.config.WarmupConns = p.config.MaxIdleConns
		if p.config.WarmupConns == 0 {
			p.config.WarmupConns = 5
		}
	}

	// Set default slow query threshold
	if p.config.SlowQueryThreshold == 0 {
		p.config.SlowQueryThreshold = 1000 // 1 second
	}

	// Set default leak detection threshold
	if p.config.LeakDetectionThreshold == 0 {
		p.config.LeakDetectionThreshold = 300 // 5 minutes
	}

	// Validate configuration
	if err := p.validateConfig(); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	return nil
}

// validateConfig validates the configuration for correctness
func (p *SQLPlugin) validateConfig() error {
	if p.config.Driver == "" {
		return fmt.Errorf("driver is required")
	}
	if p.config.DSN == "" {
		return fmt.Errorf("DSN is required")
	}
	if p.config.MaxIdleConns > p.config.MaxOpenConns {
		return fmt.Errorf("max_idle_conns (%d) cannot be greater than max_open_conns (%d)",
			p.config.MaxIdleConns, p.config.MaxOpenConns)
	}
	if p.config.MaxOpenConns <= 0 {
		return fmt.Errorf("max_open_conns must be greater than 0")
	}
	if p.config.MaxIdleConns < 0 {
		return fmt.Errorf("max_idle_conns cannot be negative")
	}
	if p.config.RetryMaxAttempts < 0 {
		return fmt.Errorf("retry_max_attempts cannot be negative")
	}
	if p.config.AlertThresholdUsage < 0 || p.config.AlertThresholdUsage > 1 {
		return fmt.Errorf("alert_threshold_usage must be between 0 and 1")
	}
	if p.config.AutoReconnectInterval < 0 {
		return fmt.Errorf("auto_reconnect_interval cannot be negative")
	}
	if p.config.AutoReconnectMaxAttempts < 0 {
		return fmt.Errorf("auto_reconnect_max_attempts cannot be negative")
	}
	if p.config.WarmupConns < 0 {
		return fmt.Errorf("warmup_conns cannot be negative")
	}
	if p.config.SlowQueryThreshold < 0 {
		return fmt.Errorf("slow_query_threshold cannot be negative")
	}
	if p.config.LeakDetectionThreshold < 0 {
		return fmt.Errorf("leak_detection_threshold cannot be negative")
	}
	return nil
}

// StartupTasks performs startup initialization with retry support
func (p *SQLPlugin) StartupTasks() error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if p.connected.Load() {
		return errors.New("already connected")
	}
	if p.closing.Load() {
		return ErrAlreadyClosed
	}

	log.Infof("Initializing database connection for %s", p.Name())

	// Attempt connection with retry if enabled
	var db *sql.DB
	var err error

	if p.config.RetryEnabled {
		db, err = p.connectWithRetryContext(p.ctx)
	} else {
		db, err = p.connectWithContext(p.ctx)
	}

	if err != nil {
		return err
	}

	dialect := p.getDialectFromDriver(p.config.Driver)
	p.mu.Lock()
	p.db = db
	p.dialect = dialect
	p.connected.Store(true)
	p.lastPingTime.Store(time.Now().Unix())
	p.mu.Unlock()

	// Warmup/maintain the pool asynchronously so plugin startup never blocks on
	// creating multiple idle connections. This is important for managed databases
	// or poolers where opening several connections may exceed the plugin startup timeout.
	if p.config.WarmupEnabled {
		go p.maintainMinPoolSize(p.ctx)
	}

	// Start health checker if configured
	if p.config.HealthCheckInterval > 0 {
		p.healthChecker = NewHealthChecker(
			p,
			time.Duration(p.config.HealthCheckInterval)*time.Second,
			p.config.HealthCheckQuery,
		)
		p.healthChecker.Start(p.ctx)
	}

	// Start pool monitor if enabled
	if p.config.MonitorEnabled {
		thresholds := &PoolThresholds{
			UsagePercentage: p.config.AlertThresholdUsage,
			WaitDuration:    time.Duration(p.config.AlertThresholdWait) * time.Second,
			WaitCount:       p.config.AlertThresholdWaitCount,
		}
		p.poolMonitor = NewPoolMonitor(
			p,
			time.Duration(p.config.MonitorInterval)*time.Second,
			thresholds,
		)
		p.poolMonitor.Start(p.ctx)
	}

	// Start auto-reconnect if enabled
	// Enable by default for production readiness (interval defaults to 5)
	// User can disable by explicitly setting auto_reconnect_enabled: false
	// Note: Since Go bool zero value is false, we can't distinguish "not set" from "explicitly false"
	// So we enable by default (production best practice) - user must explicitly disable
	if autoReconnectEnabled(p.config) {
		p.autoReconnect = NewAutoReconnector(
			p,
			time.Duration(p.config.AutoReconnectInterval)*time.Second,
			p.config.AutoReconnectMaxAttempts,
		)
		p.autoReconnect.Start(p.ctx)
		maxAttemptsStr := "unlimited"
		if p.config.AutoReconnectMaxAttempts > 0 {
			maxAttemptsStr = fmt.Sprintf("%d", p.config.AutoReconnectMaxAttempts)
		}
		log.Infof("Auto-reconnect enabled for %s (interval: %ds, max_attempts: %s)",
			p.Name(), p.config.AutoReconnectInterval, maxAttemptsStr)
	}

	// Start leak detection if enabled
	if p.config.LeakDetectionEnabled {
		p.leakDetector = NewLeakDetector(
			p,
			time.Duration(p.config.LeakDetectionThreshold)*time.Second,
		)
		p.leakDetector.Start(p.ctx)
	}

	// Initialize query monitor if enabled
	if p.config.SlowQueryEnabled {
		p.queryMonitor = NewQueryMonitor(
			true,
			time.Duration(p.config.SlowQueryThreshold)*time.Millisecond,
			p.metricsRecorder,
		)
	}

	log.Infof("Database connection established for %s", p.Name())
	p.publishResourceContract()
	return nil
}


func (p *SQLPlugin) connectWithContext(ctx context.Context) (*sql.DB, error) {
	// Record connection attempt if not retrying
	if !p.config.RetryEnabled {
		p.metricsRecorder.IncConnectAttempt()
	}

	ctx = normalizeContext(ctx)

	// Open database connection (use optional OpenDBFunc when set, e.g. for tracing)
	var db *sql.DB
	var err error
	if p.config.OpenDBFunc != nil {
		db, err = p.config.OpenDBFunc(p.config.Driver, p.config.DSN)
	} else {
		db, err = sql.Open(p.config.Driver, p.config.DSN)
	}
	if err != nil {
		if !p.config.RetryEnabled {
			p.metricsRecorder.IncConnectFailure()
		}
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool before testing connection
	db.SetMaxOpenConns(p.config.MaxOpenConns)
	db.SetMaxIdleConns(p.config.MaxIdleConns)

	if p.config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(p.config.ConnMaxLifetime) * time.Second)
	}
	if p.config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(time.Duration(p.config.ConnMaxIdleTime) * time.Second)
	}

	// Test connection with timeout (ping + execute SQL to ensure full path works)
	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(connectCtx); err != nil {
		// Ensure we close the db on ping failure to prevent resource leaks
		closeErr := db.Close()
		if closeErr != nil {
			log.Warnf("Error closing database connection after ping failure: %v", closeErr)
		}
		if !p.config.RetryEnabled {
			p.metricsRecorder.IncConnectFailure()
		}
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Verify SQL execution works (catches permission issues, proxies that block queries, etc.)
	verifyQuery := "SELECT 1"
	if p.config.HealthCheckQuery != "" {
		verifyQuery = p.config.HealthCheckQuery
	}
	var result int
	if err := db.QueryRowContext(connectCtx, verifyQuery).Scan(&result); err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			log.Warnf("Error closing database connection after SQL verification failure: %v", closeErr)
		}
		if !p.config.RetryEnabled {
			p.metricsRecorder.IncConnectFailure()
		}
		return nil, fmt.Errorf("failed to verify SQL execution (%s): %w", verifyQuery, err)
	}

	if !p.config.RetryEnabled {
		p.metricsRecorder.IncConnectSuccess()
	}

	// Update last ping time on successful connection
	p.lastPingTime.Store(time.Now().Unix())

	return db, nil
}

func (p *SQLPlugin) connectWithRetryContext(ctx context.Context) (*sql.DB, error) {
	ctx = normalizeContext(ctx)
	var lastErr error
	delay := time.Duration(p.config.RetryInitialDelay) * time.Second

	p.metricsRecorder.IncConnectAttempt()

	for attempt := 0; attempt <= p.config.RetryMaxAttempts; attempt++ {
		// Check if context is cancelled before retrying
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connection cancelled: %w", ctx.Err())
		default:
		}

		if attempt > 0 {
			log.Infof("Retrying database connection for %s (attempt %d/%d) after %v",
				p.Name(), attempt, p.config.RetryMaxAttempts, delay)
			p.metricsRecorder.IncConnectRetry()

			// Use select with context to allow cancellation during sleep
			retryTimer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return nil, fmt.Errorf("connection cancelled during retry: %w", ctx.Err())
			case <-retryTimer.C:
			}
		}

		db, err := p.connectWithContext(ctx)
		if err == nil {
			if attempt > 0 {
				log.Infof("Database connection succeeded for %s after %d retries", p.Name(), attempt)
			}
			p.metricsRecorder.IncConnectSuccess()
			return db, nil
		}

		lastErr = err

		// Calculate next delay with exponential backoff
		delay = time.Duration(float64(delay) * p.config.RetryMultiplier)
		maxDelay := time.Duration(p.config.RetryMaxDelay) * time.Second
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	p.metricsRecorder.IncConnectFailure()
	return nil, fmt.Errorf("failed to connect after %d attempts: %w", p.config.RetryMaxAttempts+1, lastErr)
}

// CleanupTasks performs cleanup on shutdown
func (p *SQLPlugin) CleanupTasks() error {
	if !p.closing.CompareAndSwap(false, true) {
		return nil
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	log.Infof("Shutting down database connection for %s", p.Name())

	// Stop background tasks
	p.cancel()

	// Stop health checker
	if p.healthChecker != nil {
		p.healthChecker.Stop()
	}

	// Stop pool monitor
	if p.poolMonitor != nil {
		p.poolMonitor.Stop()
	}

	// Stop auto-reconnect
	if p.autoReconnect != nil {
		p.autoReconnect.Stop()
	}

	// Stop leak detector
	if p.leakDetector != nil {
		p.leakDetector.Stop()
	}

	// Close database connection
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		if err := p.db.Close(); err != nil {
			log.Warnf("Error closing database connection for %s: %v", p.Name(), err)
		} else {
			log.Infof("Database connection closed for %s", p.Name())
		}
		p.db = nil
	}

	p.connected.Store(false)
	return nil
}

// GetDB returns the database connection.
// Important: when auto-reconnect is enabled, do not cache the returned *sql.DB in long-lived structs
// (e.g. global Data/Repo). After Reconnect(), the previous *sql.DB is closed; cached references will get
// "sql: database is closed". Obtain the DB at the point of use (e.g. per request) or use a provider that
// calls GetDBWithContext when needed.
func (p *SQLPlugin) GetDB() (*sql.DB, error) {
	return p.GetDBWithContext(context.Background())
}

// GetDBWithContext returns the database connection with context support.
// When EnsureAliveBeforeHandout is true (default), it pings the pool before returning; on failure
// it triggers Reconnect() once so the pool is not handed out when already broken.
// Do not cache the returned *sql.DB when auto-reconnect is enabled; after Reconnect() it becomes closed.
func (p *SQLPlugin) GetDBWithContext(ctx context.Context) (*sql.DB, error) {
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !p.IsConnectedContext(ctx) {
		if reconnectErr := p.ReconnectContext(ctx); reconnectErr != nil {
			return nil, fmt.Errorf("%w: reconnect failed: %v", ErrNotConnected, reconnectErr)
		}
	}

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	// Ensure pool is alive before handing out: ping and reconnect once on failure
	ensureAlive := p.config.EnsureAliveBeforeHandout == nil || *p.config.EnsureAliveBeforeHandout
	if ensureAlive && db != nil && !p.closing.Load() && p.shouldPingBeforeHandout() {
		pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
		err := db.PingContext(pingCtx)
		pingCancel()
		if err != nil {
			// Pool is broken or has no healthy connection; replace pool so we never hand out a closed db
			if reconnectErr := p.ReconnectContext(ctx); reconnectErr != nil {
				return nil, fmt.Errorf("pool unhealthy and reconnect failed: %w", err)
			}
			p.mu.RLock()
			db = p.db
			p.mu.RUnlock()
		} else {
			p.lastPingTime.Store(time.Now().Unix())
		}
	}

	return db, nil
}

// GetValidatedConn returns a single connection from the pool that has been verified alive (Ping).
// The returned connection is guaranteed to be usable at handoff time. Caller must call conn.Close()
// when done to return the connection to the pool.
func (p *SQLPlugin) GetValidatedConn(ctx context.Context) (*sql.Conn, error) {
	db, err := p.GetDBWithContext(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	err = conn.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		_ = conn.Close()
		// One bad connection; try reconnect and one more time
		if reconnectErr := p.ReconnectContext(ctx); reconnectErr != nil {
			return nil, fmt.Errorf("connection validation failed and reconnect failed: %w", err)
		}
		db, err = p.GetDBWithContext(ctx)
		if err != nil {
			return nil, err
		}
		conn, err = db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		pingCtx2, pingCancel2 := context.WithTimeout(ctx, 2*time.Second)
		err = conn.PingContext(pingCtx2)
		pingCancel2()
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("connection validation failed after reconnect: %w", err)
		}
	}
	return conn, nil
}

// GetDialect returns the database dialect string (e.g. "mysql", "postgres").
func (p *SQLPlugin) GetDialect() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dialect
}

// SetMetricsRecorder sets the metrics recorder for this plugin
func (p *SQLPlugin) SetMetricsRecorder(recorder MetricsRecorder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metricsRecorder = recorder
}

// GetMetricsRecorder returns the current metrics recorder
func (p *SQLPlugin) GetMetricsRecorder() MetricsRecorder {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metricsRecorder
}

// GetQueryMonitor returns the query monitor for slow query detection
func (p *SQLPlugin) GetQueryMonitor() *QueryMonitor {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.queryMonitor
}

// GetAutoReconnector returns the auto-reconnector instance
func (p *SQLPlugin) GetAutoReconnector() *AutoReconnector {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.autoReconnect
}

// CheckHealth performs a health check. When auto-reconnect is enabled it may
// rebuild the pool once on failure. When auto-reconnect is disabled, health
// checks only report the unhealthy state and never replace the current pool.
func (p *SQLPlugin) CheckHealth() error {
	return p.CheckHealthContext(context.Background())
}

func (p *SQLPlugin) CheckHealthContext(ctx context.Context) error {
	ctx = normalizeContext(ctx)
	query := "SELECT 1"
	if p.config.HealthCheckQuery != "" {
		query = p.config.HealthCheckQuery
	}

	if !p.IsConnectedContext(ctx) {
		if !autoReconnectEnabled(p.config) {
			p.metricsRecorder.RecordHealthCheck(false)
			return fmt.Errorf("database is not connected")
		}
		if err := p.ReconnectContext(ctx); err != nil {
			p.metricsRecorder.RecordHealthCheck(false)
			return err
		}
	}

	db, err := p.GetDBWithContext(ctx)
	if err != nil {
		p.metricsRecorder.RecordHealthCheck(false)
		return err
	}

	healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int
	if err := db.QueryRowContext(healthCtx, query).Scan(&result); err != nil {
		if !autoReconnectEnabled(p.config) {
			p.connected.Store(false)
			p.metricsRecorder.RecordHealthCheck(false)
			return fmt.Errorf("health check failed: %w", err)
		}
		if reconnectErr := p.ReconnectContext(ctx); reconnectErr != nil {
			p.metricsRecorder.RecordHealthCheck(false)
			return fmt.Errorf("health check failed: %w", err)
		}
		db, err = p.GetDBWithContext(ctx)
		if err != nil {
			p.metricsRecorder.RecordHealthCheck(false)
			return err
		}
		ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()
		if err2 := db.QueryRowContext(ctx2, query).Scan(&result); err2 != nil {
			p.metricsRecorder.RecordHealthCheck(false)
			return fmt.Errorf("health check failed after reconnect: %w", err2)
		}
	}

	p.metricsRecorder.RecordHealthCheck(true)
	p.lastPingTime.Store(time.Now().Unix())
	return nil
}

// IsConnected pings the database to verify the connection is live.
func (p *SQLPlugin) IsConnected() bool {
	return p.IsConnectedContext(context.Background())
}

func (p *SQLPlugin) IsConnectedContext(ctx context.Context) bool {
	if !p.connected.Load() || p.closing.Load() {
		return false
	}

	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	if db == nil {
		return false
	}

	// Perform quick ping check with short timeout
	// Use cached ping result if recent (within last 5 seconds)
	lastPing := p.lastPingTime.Load()
	now := time.Now().Unix()
	if now-lastPing < int64(connectionValidationCacheWindow/time.Second) {
		return true // Use cached result for performance
	}

	// Perform actual ping check. Keep this above typical cross-region/database
	// pooler latency so health checks do not churn managed DB connections.
	pingCtx, cancel := context.WithTimeout(normalizeContext(ctx), time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		// Connection is actually down, update state
		p.connected.Store(false)
		return false
	}

	// Update last ping time
	p.lastPingTime.Store(now)
	return true
}

// GetStats returns a snapshot of connection pool statistics.
func (p *SQLPlugin) GetStats() *ConnectionPoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if !p.connected.Load() || p.closing.Load() || p.db == nil {
		return &ConnectionPoolStats{}
	}

	stats := p.db.Stats()
	maxIdleConns := int64(p.config.MaxIdleConns)
	if maxIdleConns == 0 {
		maxIdleConns = 5 // Default value
	}

	return &ConnectionPoolStats{
		MaxOpenConnections: int64(stats.MaxOpenConnections),
		OpenConnections:    int64(stats.OpenConnections),
		InUse:              int64(stats.InUse),
		Idle:               int64(stats.Idle),
		MaxIdleConnections: maxIdleConns,
		WaitCount:          stats.WaitCount,
		WaitDuration:       stats.WaitDuration,
		MaxIdleClosed:      stats.MaxIdleClosed,
		MaxLifetimeClosed:  stats.MaxLifetimeClosed,
	}
}

func autoReconnectEnabled(config *interfaces.Config) bool {
	if config == nil || config.AutoReconnectInterval <= 0 {
		return false
	}
	return config.AutoReconnectEnabled == nil || *config.AutoReconnectEnabled
}

func (p *SQLPlugin) shouldPingBeforeHandout() bool {
	lastPing := p.lastPingTime.Load()
	if lastPing == 0 {
		return true
	}
	return time.Now().Unix()-lastPing >= int64(connectionValidationCacheWindow/time.Second)
}

// getDialectFromDriver determines the dialect from the driver name
func (p *SQLPlugin) getDialectFromDriver(driver string) string {
	dialectMap := map[string]string{
		"mysql":      "mysql",
		"postgres":   "postgres",
		"pgx":        "postgres",
		"mssql":      "mssql",
		"sqlserver":  "mssql",
		"sqlite3":    "sqlite",
		"sqlite":     "sqlite",
		"clickhouse": "clickhouse",
	}

	if dialect, ok := dialectMap[driver]; ok {
		return dialect
	}
	return driver
}

// Reconnect re-establishes the database connection; called by AutoReconnector on failure.
func (p *SQLPlugin) Reconnect() error {
	return p.ReconnectContext(p.ctx)
}

func (p *SQLPlugin) ReconnectContext(ctx context.Context) error {
	ctx = normalizeContext(ctx)
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	p.mu.Lock()
	if p.closing.Load() {
		p.mu.Unlock()
		return ErrAlreadyClosed
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("reconnection canceled: %w", err)
	}

	log.Debugf("Attempting to reconnect database for %s", p.Name())

	// Keep the old pool until the replacement has been opened and verified.
	// This avoids turning a failed reconnect attempt into an immediate resource drop.
	oldDB := p.db
	p.connected.Store(false)
	p.mu.Unlock()

	// Attempt to reconnect
	// Note: connect() and connectWithRetry() use p.ctx for timeouts
	// p.ctx should still be valid during reconnection (only cancelled on plugin shutdown)
	var db *sql.DB
	var err error

	if p.config.RetryEnabled {
		// Use retry mechanism for reconnection
		db, err = p.connectWithRetryContext(ctx)
	} else {
		// Single attempt
		db, err = p.connectWithContext(ctx)
	}

	if err != nil {
		return fmt.Errorf("reconnection failed: %w", err)
	}

	// Update connection state
	p.mu.Lock()
	p.db = db
	p.connected.Store(true)
	p.lastPingTime.Store(time.Now().Unix())
	p.mu.Unlock()
	if oldDB != nil {
		_ = oldDB.Close()
	}

	log.Debugf("Successfully reconnected database for %s", p.Name())
	p.publishResourceContract()
	return nil
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func (p *SQLPlugin) maintainMinPoolSize(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic in pool warmup goroutine for %s: %v", p.Name(), r)
		}
	}()
	interval := p.minPoolMaintainInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	tryEnsure := func(logSuccess bool) {
		target := p.targetWarmConnections()
		if target <= 0 || p.closing.Load() || !p.connected.Load() {
			return
		}
		if err := p.ensureMinPoolConnections(ctx, target); err != nil {
			log.Debugf("Failed to maintain minimum pool size for %s: %v", p.Name(), err)
			return
		}
		if logSuccess {
			log.Infof("Connection pool warmed up for %s: target=%d", p.Name(), target)
		}
	}

	// Run one immediate best-effort warmup so min_conn starts taking effect right
	// after startup, but without blocking StartupTasks().
	tryEnsure(true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tryEnsure(false)
		}
	}
}

func (p *SQLPlugin) minPoolMaintainInterval() time.Duration {
	if p.config.ConnMaxIdleTime <= 0 {
		return minPoolMaintainDefaultInterval
	}

	interval := time.Duration(p.config.ConnMaxIdleTime) * time.Second / 2
	if interval < minPoolMaintainMinInterval {
		return minPoolMaintainMinInterval
	}
	if interval > minPoolMaintainMaxInterval {
		return minPoolMaintainMaxInterval
	}
	return interval
}

func (p *SQLPlugin) targetWarmConnections() int {
	target := p.config.WarmupConns
	if target <= 0 {
		return 0
	}
	if p.config.MaxOpenConns > 0 && target > p.config.MaxOpenConns {
		target = p.config.MaxOpenConns
	}
	if p.config.MaxIdleConns > 0 && target > p.config.MaxIdleConns {
		target = p.config.MaxIdleConns
	}
	return target
}

func (p *SQLPlugin) ensureMinPoolConnections(parent context.Context, target int) error {
	if target <= 0 {
		return nil
	}

	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	stats := db.Stats()
	if stats.OpenConnections >= target {
		return nil
	}

	deficit := target - stats.OpenConnections
	timeout := time.Duration(deficit) * minPoolSingleConnectionPingWindow
	if timeout < minPoolSingleConnectionPingWindow {
		timeout = minPoolSingleConnectionPingWindow
	}
	if timeout > minPoolConnectionWarmupTimeout {
		timeout = minPoolConnectionWarmupTimeout
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	heldConns := make([]*sql.Conn, 0, deficit)
	release := func() {
		for _, conn := range heldConns {
			_ = conn.Close()
		}
	}
	defer release()

	for i := 0; i < deficit; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("acquire warmup connection %d/%d: %w", i+1, deficit, err)
		}

		pingCtx, pingCancel := context.WithTimeout(ctx, minPoolSingleConnectionPingWindow)
		err = conn.PingContext(pingCtx)
		pingCancel()
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("ping warmup connection %d/%d: %w", i+1, deficit, err)
		}

		heldConns = append(heldConns, conn)
	}

	return nil
}
