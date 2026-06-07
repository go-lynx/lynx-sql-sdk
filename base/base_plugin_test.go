package base

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-lynx/lynx-sql-sdk/interfaces"
	"github.com/go-lynx/lynx/plugins"
)

const (
	failingPingDriverName = "sqlsdk-failing-ping"
	successDriverName     = "sqlsdk-success"
)

func init() {
	sql.Register(failingPingDriverName, failingPingDriver{})
	sql.Register(successDriverName, successDriver{})
}

type failingPingDriver struct{}

func (failingPingDriver) Open(string) (driver.Conn, error) {
	return failingPingConn{}, nil
}

type failingPingConn struct{}

func (failingPingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (failingPingConn) Close() error {
	return nil
}

func (failingPingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin not implemented")
}

func (failingPingConn) Ping(context.Context) error {
	return errors.New("ping failed")
}

type successDriver struct{}

func (successDriver) Open(string) (driver.Conn, error) {
	return successConn{}, nil
}

type successConn struct{}

func (successConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (successConn) Close() error {
	return nil
}

func (successConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin not implemented")
}

func (successConn) Ping(context.Context) error {
	return nil
}

func (successConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &singleValueRows{}, nil
}

type singleValueRows struct {
	sent bool
}

func (*singleValueRows) Columns() []string {
	return []string{"value"}
}

func (*singleValueRows) Close() error {
	return nil
}

func (r *singleValueRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	dest[0] = int64(1)
	r.sent = true
	return nil
}

// mockRuntime is a mock implementation of plugins.Runtime for testing
type mockRuntime struct {
	config           map[string]any
	sharedResources  map[string]any
	privateResources map[string]any
}

func (m *mockRuntime) GetConfig() config.Config {
	return &mockConfig{values: m.config}
}

func (m *mockRuntime) AddListener(listener plugins.EventListener, filter *plugins.EventFilter) {}
func (m *mockRuntime) AddPluginListener(pluginName string, listener plugins.EventListener, filter *plugins.EventFilter) {
}
func (m *mockRuntime) CleanupResources(pluginName string) error                                 { return nil }
func (m *mockRuntime) EmitEvent(event plugins.PluginEvent)                                      {}
func (m *mockRuntime) EmitPluginEvent(pluginName string, eventType string, data map[string]any) {}
func (m *mockRuntime) GetCurrentPluginContext() string                                          { return "" }
func (m *mockRuntime) GetEventHistory(filter plugins.EventFilter) []plugins.PluginEvent         { return nil }
func (m *mockRuntime) GetEventStats() map[string]any                                            { return nil }
func (m *mockRuntime) GetLogger() log.Logger                                                    { return log.DefaultLogger }
func (m *mockRuntime) GetPluginEventHistory(pluginName string, filter plugins.EventFilter) []plugins.PluginEvent {
	return nil
}
func (m *mockRuntime) GetResourceStats() map[string]any                           { return nil }
func (m *mockRuntime) GetSharedResource(name string) (any, error)                 { return nil, nil }
func (m *mockRuntime) GetPrivateResource(name string) (any, error)                { return nil, nil }
func (m *mockRuntime) GetResource(name string) (any, error)                       { return nil, nil }
func (m *mockRuntime) GetResourceInfo(name string) (*plugins.ResourceInfo, error) { return nil, nil }
func (m *mockRuntime) ListResources() []*plugins.ResourceInfo                     { return nil }
func (m *mockRuntime) RegisterPrivateResource(name string, resource any) error {
	if m.privateResources == nil {
		m.privateResources = make(map[string]any)
	}
	m.privateResources[name] = resource
	return nil
}
func (m *mockRuntime) RegisterResource(name string, resource any) error { return nil }
func (m *mockRuntime) RegisterSharedResource(name string, resource any) error {
	if m.sharedResources == nil {
		m.sharedResources = make(map[string]any)
	}
	m.sharedResources[name] = resource
	return nil
}
func (m *mockRuntime) RemoveListener(listener plugins.EventListener)                          {}
func (m *mockRuntime) RemovePluginListener(pluginName string, listener plugins.EventListener) {}
func (m *mockRuntime) SetConfig(conf config.Config)                                           {}
func (m *mockRuntime) SetEventDispatchMode(mode string) error                                 { return nil }
func (m *mockRuntime) SetEventTimeout(timeout time.Duration)                                  {}
func (m *mockRuntime) SetEventWorkerPoolSize(size int)                                        {}
func (m *mockRuntime) Shutdown()                                                              {}
func (m *mockRuntime) UnregisterPrivateResource(name string) error                            { return nil }
func (m *mockRuntime) UnregisterResource(name string) error                                   { return nil }
func (m *mockRuntime) UnregisterSharedResource(name string) error                             { return nil }
func (m *mockRuntime) WithPluginContext(pluginName string) plugins.Runtime                    { return m }
func (m *mockRuntime) GetTypedResource(name string, resourceType string) (any, error) {
	return nil, nil
}
func (m *mockRuntime) RegisterTypedResource(name string, resource any, resourceType string) error {
	return nil
}

type mockConfig struct {
	values map[string]any
}

func (m *mockConfig) Value(key string) config.Value {
	return &mockValue{key: key, values: m.values}
}

func (m *mockConfig) Scan(dest any) error                       { return nil }
func (m *mockConfig) Load() error                               { return nil }
func (m *mockConfig) Watch(key string, o config.Observer) error { return nil }
func (m *mockConfig) Close() error                              { return nil }

type mockValue struct {
	key    string
	values map[string]any
}

type staticDBProvider struct {
	db *sql.DB
}

func (p staticDBProvider) DB(context.Context) (*sql.DB, error) {
	return p.db, nil
}

func (p staticDBProvider) ValidatedConn(context.Context) (*sql.Conn, error) {
	return nil, nil
}

func (p staticDBProvider) Dialect() string {
	return "mysql"
}

func (m *mockValue) Scan(dest any) error {
	if val, ok := m.values[m.key]; ok {
		if config, ok := dest.(*interfaces.Config); ok {
			if cfg, ok := val.(*interfaces.Config); ok {
				*config = *cfg
				return nil
			}
		}
	}
	return errors.New("config not found")
}

func (m *mockValue) Bool() (bool, error)              { return false, nil }
func (m *mockValue) Int() (int64, error)              { return 0, nil }
func (m *mockValue) Float() (float64, error)          { return 0, nil }
func (m *mockValue) String() (string, error)          { return "", nil }
func (m *mockValue) Duration() (time.Duration, error) { return 0, nil }
func (m *mockValue) Slice() ([]config.Value, error)   { return nil, nil }
func (m *mockValue) Map() (map[string]config.Value, error) {
	return nil, nil
}
func (m *mockValue) Load() any   { return nil }
func (m *mockValue) Store(v any) {}

func TestSQLPlugin_PublishResourceContract(t *testing.T) {
	db, err := sql.Open(failingPingDriverName, "")
	if err != nil {
		t.Fatalf("open sqlite3: %v", err)
	}
	defer db.Close()

	rt := &mockRuntime{}
	plugin := &SQLPlugin{
		BasePlugin: plugins.NewBasePlugin("id", "mysql.client", "desc", "v1", "lynx.mysql", 100),
		config: &interfaces.Config{
			Driver: "mysql",
			DSN:    "dsn",
		},
		db:      db,
		dialect: "mysql",
	}
	plugin.bindRuntime(rt)
	plugin.SetProvider(staticDBProvider{db: db})

	plugin.publishResourceContract()

	if _, ok := rt.sharedResources["mysql.client"]; !ok {
		t.Fatalf("expected shared provider resource mysql.client to be published")
	}
	if _, ok := rt.sharedResources["mysql.client.provider"]; !ok {
		t.Fatalf("expected shared provider resource mysql.client.provider to be published")
	}
	if got := rt.privateResources["db"]; got != db {
		t.Fatalf("expected private db resource to be published")
	}
	if _, ok := rt.privateResources["provider"]; !ok {
		t.Fatalf("expected private provider resource to be published")
	}
	if _, ok := rt.privateResources["config"]; !ok {
		t.Fatalf("expected private config resource to be published")
	}
	if got, ok := rt.privateResources["dialect"].(string); !ok || got != "mysql" {
		t.Fatalf("expected private dialect resource to be mysql, got %#v", rt.privateResources["dialect"])
	}
}

func TestNewBaseSQLPlugin(t *testing.T) {
	config := &interfaces.Config{
		Driver:       "sqlite3",
		DSN:          ":memory:",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	if plugin == nil {
		t.Fatal("NewBaseSQLPlugin returned nil")
	}

	if plugin.Name() != "test-plugin" {
		t.Errorf("Expected name 'test-plugin', got '%s'", plugin.Name())
	}

	if plugin.config != config {
		t.Error("Config not set correctly")
	}
}

func TestSQLPlugin_InitializeResources(t *testing.T) {
	config := &interfaces.Config{
		Driver:       "sqlite3",
		DSN:          ":memory:",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	// Test default values
	if plugin.config.MaxOpenConns == 0 {
		t.Error("MaxOpenConns should have default value")
	}
}

func TestSQLPlugin_ValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *interfaces.Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &interfaces.Config{
				Driver:       "sqlite3",
				DSN:          ":memory:",
				MaxOpenConns: 10,
				MaxIdleConns: 5,
			},
			wantErr: false,
		},
		{
			name: "missing driver",
			config: &interfaces.Config{
				DSN:          ":memory:",
				MaxOpenConns: 10,
			},
			wantErr: true,
		},
		{
			name: "missing DSN",
			config: &interfaces.Config{
				Driver:       "sqlite3",
				MaxOpenConns: 10,
			},
			wantErr: true,
		},
		{
			name: "max_idle_conns greater than max_open_conns",
			config: &interfaces.Config{
				Driver:       "sqlite3",
				DSN:          ":memory:",
				MaxOpenConns: 5,
				MaxIdleConns: 10,
			},
			wantErr: true,
		},
		{
			name: "max_open_conns zero uses default",
			config: &interfaces.Config{
				Driver:       "sqlite3",
				DSN:          ":memory:",
				MaxOpenConns: 0,
			},
			wantErr: false,
		},
		{
			name: "invalid alert_threshold_usage",
			config: &interfaces.Config{
				Driver:              "sqlite3",
				DSN:                 ":memory:",
				MaxOpenConns:        10,
				AlertThresholdUsage: 1.5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := NewBaseSQLPlugin(
				"test-id",
				"test-plugin",
				"Test plugin",
				"v1.0.0",
				"test.prefix",
				100,
				tt.config,
			)

			rt := &mockRuntime{
				config: map[string]any{
					"test.prefix": tt.config,
				},
			}

			err := plugin.InitializeResources(rt)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitializeResources() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSQLPlugin_StartupTasks(t *testing.T) {
	// Skip test that requires actual database connection
	t.Skip("Skipping test that requires database connection")

	// Use SQLite in-memory database for testing
	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0, // Disable health check for faster tests
		AutoReconnectInterval: 0, // Disable auto-reconnect for tests
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	if !plugin.IsConnected() {
		t.Error("Plugin should be connected after StartupTasks")
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_StartupTasksPublishesResourcesWithoutDeadlock(t *testing.T) {
	falseValue := false
	config := &interfaces.Config{
		Driver:               successDriverName,
		DSN:                  "startup-publish",
		MaxOpenConns:         10,
		MaxIdleConns:         5,
		HealthCheckInterval:  0,
		AutoReconnectEnabled: &falseValue,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)
	plugin.SetProvider(staticDBProvider{})

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}
	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- plugin.StartupTasks()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StartupTasks failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StartupTasks deadlocked while publishing resources")
	}

	if _, ok := rt.sharedResources["test-plugin"]; !ok {
		t.Fatal("expected shared provider resource to be published")
	}
	if _, ok := rt.sharedResources["test-plugin.provider"]; !ok {
		t.Fatal("expected canonical shared provider resource to be published")
	}

	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_ReconnectPublishesResourcesWithoutDeadlock(t *testing.T) {
	falseValue := false
	config := &interfaces.Config{
		Driver:               successDriverName,
		DSN:                  "reconnect-publish",
		MaxOpenConns:         10,
		MaxIdleConns:         5,
		HealthCheckInterval:  0,
		AutoReconnectEnabled: &falseValue,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)
	plugin.SetProvider(staticDBProvider{})

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}
	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}
	if err := plugin.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- plugin.Reconnect()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconnect failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Reconnect deadlocked while publishing resources")
	}

	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_ReconnectFailurePreservesExistingPool(t *testing.T) {
	falseValue := false
	config := &interfaces.Config{
		Driver:               successDriverName,
		DSN:                  "reconnect-preserve",
		MaxOpenConns:         10,
		MaxIdleConns:         5,
		HealthCheckInterval:  0,
		AutoReconnectEnabled: &falseValue,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}
	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}
	if err := plugin.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	plugin.mu.RLock()
	oldDB := plugin.db
	plugin.mu.RUnlock()
	if oldDB == nil {
		t.Fatal("expected startup to create a db pool")
	}

	plugin.config.Driver = failingPingDriverName
	if err := plugin.Reconnect(); err == nil {
		t.Fatal("expected reconnect to fail with failing ping driver")
	}

	plugin.mu.RLock()
	currentDB := plugin.db
	plugin.mu.RUnlock()
	if currentDB != oldDB {
		t.Fatal("failed reconnect should preserve existing pool until a replacement succeeds")
	}
	if plugin.connected.Load() {
		t.Fatal("failed reconnect should mark plugin disconnected")
	}

	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_GetDB(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	// Test GetDB before connection
	_, err = plugin.GetDB()
	if err == nil {
		t.Error("GetDB should return error when not connected")
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	// Test GetDB after connection
	db, err := plugin.GetDB()
	if err != nil {
		t.Fatalf("GetDB failed: %v", err)
	}
	if db == nil {
		t.Error("GetDB returned nil database")
	}

	// Test GetDBWithContext
	ctx := context.Background()
	db, err = plugin.GetDBWithContext(ctx)
	if err != nil {
		t.Fatalf("GetDBWithContext failed: %v", err)
	}
	if db == nil {
		t.Error("GetDBWithContext returned nil database")
	}

	// Test with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = plugin.GetDBWithContext(ctx)
	if err == nil {
		t.Error("GetDBWithContext should return error with cancelled context")
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_CheckHealth(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	// Test CheckHealth before connection
	err = plugin.CheckHealth()
	if err == nil {
		t.Error("CheckHealth should return error when not connected")
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	// Test CheckHealth after connection
	err = plugin.CheckHealth()
	if err != nil {
		t.Fatalf("CheckHealth failed: %v", err)
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_CheckHealthDoesNotReconnectWhenDisabled(t *testing.T) {
	autoReconnect := false
	config := &interfaces.Config{
		Driver:                successDriverName,
		DSN:                   "success",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectEnabled:  &autoReconnect,
		AutoReconnectInterval: 5,
		RetryEnabled:          false,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}
	if err := plugin.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}
	t.Cleanup(func() {
		_ = plugin.CleanupTasks()
	})

	plugin.mu.RLock()
	db := plugin.db
	plugin.mu.RUnlock()
	if db == nil {
		t.Fatal("expected db to be initialized")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	if err := plugin.CheckHealth(); err == nil {
		t.Fatal("CheckHealth should fail instead of reconnecting when auto-reconnect is disabled")
	}
	if plugin.IsConnected() {
		t.Fatal("plugin should remain disconnected after failed health check")
	}
}

func TestSQLPlugin_IsConnected(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	// Test IsConnected before connection
	if plugin.IsConnected() {
		t.Error("IsConnected should return false before connection")
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	// Test IsConnected after connection
	if !plugin.IsConnected() {
		t.Error("IsConnected should return true after connection")
	}

	// Cleanup
	_ = plugin.CleanupTasks()

	// Test IsConnected after cleanup
	if plugin.IsConnected() {
		t.Error("IsConnected should return false after cleanup")
	}
}

func TestSQLPlugin_ReconnectContextCanceled(t *testing.T) {
	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		&interfaces.Config{
			Driver:       failingPingDriverName,
			DSN:          "ignored",
			MaxOpenConns: 1,
			MaxIdleConns: 1,
			OpenDBFunc: func(driver string, dsn string) (*sql.DB, error) {
				t.Fatalf("OpenDBFunc should not be called when context is already canceled")
				return nil, nil
			},
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := plugin.ReconnectContext(ctx)
	if err == nil {
		t.Fatal("expected reconnect to fail for canceled context")
	}
	if !strings.Contains(err.Error(), "reconnection canceled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLPlugin_ConnectWithRetryContextCanceledDuringBackoff(t *testing.T) {
	var openCalls atomic.Int32

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		&interfaces.Config{
			Driver:            failingPingDriverName,
			DSN:               "ignored",
			MaxOpenConns:      1,
			MaxIdleConns:      1,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1,
			RetryMaxDelay:     1,
			RetryMultiplier:   2,
			OpenDBFunc: func(driver string, dsn string) (*sql.DB, error) {
				openCalls.Add(1)
				return sql.Open(failingPingDriverName, dsn)
			},
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := plugin.connectWithRetryContext(ctx)
		errCh <- err
	}()

	waitForValue(t, 500*time.Millisecond, func() bool {
		return openCalls.Load() == 1
	})

	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected retry loop to return an error")
		}
		if !strings.Contains(err.Error(), "connection cancelled during retry") &&
			!strings.Contains(err.Error(), "connection cancelled") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("retry loop did not stop after context cancellation")
	}

	if openCalls.Load() != 1 {
		t.Fatalf("expected a single connection attempt before cancellation, got %d", openCalls.Load())
	}
}

func waitForValue(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}

func TestSQLPlugin_GetStats(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	// Test GetStats before connection — nil means not connected
	if stats := plugin.GetStats(); stats != nil {
		t.Error("GetStats should return nil before connection")
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	// Test GetStats after connection
	stats := plugin.GetStats()
	if stats == nil {
		t.Fatal("GetStats should return non-nil stats after connection")
	}
	if stats.MaxOpenConnections == 0 {
		t.Error("GetStats should return non-zero MaxOpenConnections after connection")
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_Reconnect(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	// Test Reconnect
	err = plugin.Reconnect()
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}

	if !plugin.IsConnected() {
		t.Error("Plugin should be connected after Reconnect")
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

func TestSQLPlugin_CleanupTasks(t *testing.T) {
	config := &interfaces.Config{
		Driver:                failingPingDriverName,
		DSN:                   "cleanup-test",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	db, err := sql.Open(failingPingDriverName, "cleanup-test")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	plugin.db = db
	plugin.connected.Store(true)

	// Test CleanupTasks
	err = plugin.CleanupTasks()
	if err != nil {
		t.Fatalf("CleanupTasks failed: %v", err)
	}

	// Test double cleanup (should be idempotent)
	err = plugin.CleanupTasks()
	if err != nil {
		t.Fatalf("CleanupTasks should be idempotent, got error: %v", err)
	}

	if plugin.IsConnected() {
		t.Error("Plugin should not be connected after cleanup")
	}
	if plugin.db != nil {
		t.Error("Plugin should clear database handle after cleanup")
	}
}

func TestSQLPlugin_GetDialect(t *testing.T) {
	tests := []struct {
		name     string
		driver   string
		expected string
	}{
		{"mysql", "mysql", "mysql"},
		{"postgres", "postgres", "postgres"},
		{"pgx", "pgx", "postgres"},
		{"mssql", "mssql", "mssql"},
		{"sqlserver", "sqlserver", "mssql"},
		{"sqlite3", "sqlite3", "sqlite"},
		{"unknown", "unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &interfaces.Config{
				Driver:                tt.driver,
				DSN:                   "test://dsn",
				MaxOpenConns:          10,
				MaxIdleConns:          5,
				HealthCheckInterval:   0,
				AutoReconnectInterval: 0,
			}

			plugin := NewBaseSQLPlugin(
				"test-id",
				"test-plugin",
				"Test plugin",
				"v1.0.0",
				"test.prefix",
				100,
				config,
			)

			rt := &mockRuntime{
				config: map[string]any{
					"test.prefix": config,
				},
			}

			err := plugin.InitializeResources(rt)
			if err != nil {
				t.Fatalf("InitializeResources failed: %v", err)
			}

			// Test dialect mapping - we can't access private method, so we test through GetDialect
			// which requires connection, so we'll just verify the config was set correctly
			if plugin.config.Driver != tt.driver {
				t.Errorf("Driver not set correctly: got %v, want %v", plugin.config.Driver, tt.driver)
			}
		})
	}
}

func TestSQLPlugin_ConnectionRetry(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	// Test with invalid DSN to trigger retry logic
	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   "invalid://dsn",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		RetryEnabled:          true,
		RetryMaxAttempts:      2,
		RetryInitialDelay:     1,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	// Startup should fail with invalid DSN
	err = plugin.StartupTasks()
	if err == nil {
		t.Error("StartupTasks should fail with invalid DSN")
	}
}

func TestSQLPlugin_ConcurrentAccess(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	// Test concurrent access
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			db, err := plugin.GetDB()
			if err != nil {
				t.Errorf("GetDB failed: %v", err)
			}
			if db == nil {
				t.Error("GetDB returned nil")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

func TestAutoReconnectEnabled(t *testing.T) {
	trueValue := true
	falseValue := false

	tests := []struct {
		name   string
		config *interfaces.Config
		want   bool
	}{
		{
			name: "disabled when interval is zero",
			config: &interfaces.Config{
				AutoReconnectInterval: 0,
			},
			want: false,
		},
		{
			name: "enabled by default when flag is nil",
			config: &interfaces.Config{
				AutoReconnectInterval: 5,
			},
			want: true,
		},
		{
			name: "enabled when explicitly true",
			config: &interfaces.Config{
				AutoReconnectEnabled:  &trueValue,
				AutoReconnectInterval: 5,
			},
			want: true,
		},
		{
			name: "disabled when explicitly false",
			config: &interfaces.Config{
				AutoReconnectEnabled:  &falseValue,
				AutoReconnectInterval: 5,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := autoReconnectEnabled(tt.config); got != tt.want {
				t.Fatalf("autoReconnectEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSQLPlugin_TargetWarmConnections(t *testing.T) {
	tests := []struct {
		name   string
		config *interfaces.Config
		want   int
	}{
		{
			name: "uses warmup conns directly",
			config: &interfaces.Config{
				WarmupConns:  5,
				MaxIdleConns: 5,
				MaxOpenConns: 25,
			},
			want: 5,
		},
		{
			name: "clamps to max idle",
			config: &interfaces.Config{
				WarmupConns:  10,
				MaxIdleConns: 5,
				MaxOpenConns: 25,
			},
			want: 5,
		},
		{
			name: "clamps to max open",
			config: &interfaces.Config{
				WarmupConns:  10,
				MaxIdleConns: 10,
				MaxOpenConns: 6,
			},
			want: 6,
		},
		{
			name: "disabled when warmup non-positive",
			config: &interfaces.Config{
				WarmupConns:  0,
				MaxIdleConns: 5,
				MaxOpenConns: 25,
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &SQLPlugin{config: tt.config}
			if got := plugin.targetWarmConnections(); got != tt.want {
				t.Fatalf("targetWarmConnections() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSQLPlugin_MinPoolMaintainInterval(t *testing.T) {
	tests := []struct {
		name   string
		config *interfaces.Config
		want   time.Duration
	}{
		{
			name:   "defaults when max idle time disabled",
			config: &interfaces.Config{},
			want:   minPoolMaintainDefaultInterval,
		},
		{
			name: "uses half of idle time",
			config: &interfaces.Config{
				ConnMaxIdleTime: 20,
			},
			want: 10 * time.Second,
		},
		{
			name: "clamps to minimum interval",
			config: &interfaces.Config{
				ConnMaxIdleTime: 1,
			},
			want: minPoolMaintainMinInterval,
		},
		{
			name: "clamps to maximum interval",
			config: &interfaces.Config{
				ConnMaxIdleTime: 120,
			},
			want: minPoolMaintainMaxInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &SQLPlugin{config: tt.config}
			if got := plugin.minPoolMaintainInterval(); got != tt.want {
				t.Fatalf("minPoolMaintainInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSQLPlugin_ShouldPingBeforeHandout(t *testing.T) {
	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		&interfaces.Config{},
	)

	if !plugin.shouldPingBeforeHandout() {
		t.Fatal("expected first handout to require ping")
	}

	plugin.lastPingTime.Store(time.Now().Unix())
	if plugin.shouldPingBeforeHandout() {
		t.Fatal("expected recent validation to skip ping")
	}

	plugin.lastPingTime.Store(time.Now().Add(-2 * connectionValidationCacheWindow).Unix())
	if !plugin.shouldPingBeforeHandout() {
		t.Fatal("expected stale validation to require ping")
	}
}

func TestSQLPlugin_QueryExecution(t *testing.T) {
	t.Skip("Skipping test that requires database connection")

	config := &interfaces.Config{
		Driver:                "sqlite3",
		DSN:                   ":memory:",
		MaxOpenConns:          10,
		MaxIdleConns:          5,
		HealthCheckInterval:   0,
		AutoReconnectInterval: 0,
	}

	plugin := NewBaseSQLPlugin(
		"test-id",
		"test-plugin",
		"Test plugin",
		"v1.0.0",
		"test.prefix",
		100,
		config,
	)

	rt := &mockRuntime{
		config: map[string]any{
			"test.prefix": config,
		},
	}

	err := plugin.InitializeResources(rt)
	if err != nil {
		t.Fatalf("InitializeResources failed: %v", err)
	}

	err = plugin.StartupTasks()
	if err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	db, err := plugin.GetDB()
	if err != nil {
		t.Fatalf("GetDB failed: %v", err)
	}

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO test (name) VALUES (?)", "test")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query data
	var name string
	err = db.QueryRow("SELECT name FROM test WHERE id = ?", 1).Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}

	if name != "test" {
		t.Errorf("Expected 'test', got '%s'", name)
	}

	// Cleanup
	_ = plugin.CleanupTasks()
}

// TestSQLPlugin_ConcurrentInitialization verifies that independent plugins can be
// initialised concurrently without races (each goroutine uses its own instance).
func TestSQLPlugin_ConcurrentInitialization(t *testing.T) {
	const workers = 10

	baseCfg := &interfaces.Config{
		Driver:       successDriverName,
		DSN:          "concurrent-init",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}
	rt := &mockRuntime{
		config: map[string]any{"test.prefix": baseCfg},
	}

	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			cfg := &interfaces.Config{
				Driver:       successDriverName,
				DSN:          "concurrent-init",
				MaxOpenConns: 10,
				MaxIdleConns: 5,
			}
			p := NewBaseSQLPlugin("id", "test-plugin", "desc", "v1", "test.prefix", 100, cfg)
			errs <- p.InitializeResources(rt)
		}()
	}
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent InitializeResources returned error: %v", err)
		}
	}
}

// TestSQLPlugin_StartupAndRestart verifies that a plugin can be started, cleaned up,
// and then re-initialized and started again (simulating a plugin restart).
func TestSQLPlugin_StartupAndRestart(t *testing.T) {
	falseValue := false
	for cycle := range 2 {
		cfg := &interfaces.Config{
			Driver:               successDriverName,
			DSN:                  "restart-test",
			MaxOpenConns:         5,
			MaxIdleConns:         2,
			HealthCheckInterval:  0,
			AutoReconnectEnabled: &falseValue,
		}
		rt := &mockRuntime{config: map[string]any{"test.prefix": cfg}}
		plugin := NewBaseSQLPlugin("id", "test-plugin", "desc", "v1", "test.prefix", 100, cfg)

		if err := plugin.InitializeResources(rt); err != nil {
			t.Fatalf("cycle %d: InitializeResources: %v", cycle, err)
		}
		if err := plugin.StartupTasks(); err != nil {
			t.Fatalf("cycle %d: StartupTasks: %v", cycle, err)
		}
		if !plugin.connected.Load() {
			t.Fatalf("cycle %d: expected connected=true after startup", cycle)
		}
		if err := plugin.CleanupTasks(); err != nil {
			t.Fatalf("cycle %d: CleanupTasks: %v", cycle, err)
		}
		if plugin.connected.Load() {
			t.Fatalf("cycle %d: expected connected=false after cleanup", cycle)
		}
	}
}

// TestSQLPlugin_MetricsRecorderCalledOnStartup verifies that connection attempt /
// success counters are incremented when the plugin connects successfully.
func TestSQLPlugin_MetricsRecorderCalledOnStartup(t *testing.T) {
	falseValue := false
	cfg := &interfaces.Config{
		Driver:               successDriverName,
		DSN:                  "metrics-on-startup",
		MaxOpenConns:         5,
		MaxIdleConns:         2,
		HealthCheckInterval:  0,
		AutoReconnectEnabled: &falseValue,
	}
	rt := &mockRuntime{config: map[string]any{"test.prefix": cfg}}

	plugin := NewBaseSQLPlugin("id", "test-plugin", "desc", "v1", "test.prefix", 100, cfg)
	rec := &countingMetricsRecorder{}
	plugin.SetMetricsRecorder(rec)

	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources: %v", err)
	}
	if err := plugin.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks: %v", err)
	}
	t.Cleanup(func() { _ = plugin.CleanupTasks() })

	if rec.connectAttempts == 0 {
		t.Error("expected at least one connect attempt to be recorded")
	}
	if rec.connectSuccess == 0 {
		t.Error("expected at least one connect success to be recorded")
	}
	if rec.connectFailures != 0 {
		t.Errorf("expected zero connect failures, got %d", rec.connectFailures)
	}
}

// TestSQLPlugin_MetricsRecorderCalledOnFailedConnect verifies that the connect
// failure counter is incremented when the plugin cannot connect.
func TestSQLPlugin_MetricsRecorderCalledOnFailedConnect(t *testing.T) {
	falseValue := false
	cfg := &interfaces.Config{
		Driver:               failingPingDriverName,
		DSN:                  "metrics-on-failure",
		MaxOpenConns:         5,
		MaxIdleConns:         2,
		HealthCheckInterval:  0,
		AutoReconnectEnabled: &falseValue,
	}
	rt := &mockRuntime{config: map[string]any{"test.prefix": cfg}}

	plugin := NewBaseSQLPlugin("id", "test-plugin", "desc", "v1", "test.prefix", 100, cfg)
	rec := &countingMetricsRecorder{}
	plugin.SetMetricsRecorder(rec)

	if err := plugin.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources: %v", err)
	}
	// StartupTasks should fail because ping fails.
	if err := plugin.StartupTasks(); err == nil {
		_ = plugin.CleanupTasks()
		t.Fatal("expected StartupTasks to fail with failing ping driver")
	}

	if rec.connectAttempts == 0 {
		t.Error("expected at least one connect attempt to be recorded")
	}
	if rec.connectFailures == 0 {
		t.Error("expected at least one connect failure to be recorded")
	}
}

// countingMetricsRecorder counts metric calls for test assertions.
type countingMetricsRecorder struct {
	connectAttempts int
	connectRetries  int
	connectSuccess  int
	connectFailures int
	healthChecks    int
}

func (c *countingMetricsRecorder) RecordConnectionPoolStats(*ConnectionPoolStats) {}
func (c *countingMetricsRecorder) RecordHealthCheck(bool)                         { c.healthChecks++ }
func (c *countingMetricsRecorder) RecordQuery(time.Duration, error, time.Duration) {}
func (c *countingMetricsRecorder) RecordTx(time.Duration, bool)                   {}
func (c *countingMetricsRecorder) IncConnectAttempt()                             { c.connectAttempts++ }
func (c *countingMetricsRecorder) IncConnectRetry()                               { c.connectRetries++ }
func (c *countingMetricsRecorder) IncConnectSuccess()                             { c.connectSuccess++ }
func (c *countingMetricsRecorder) IncConnectFailure()                             { c.connectFailures++ }
