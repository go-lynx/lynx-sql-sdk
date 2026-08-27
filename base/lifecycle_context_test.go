package base

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-lynx/lynx-sql-sdk/interfaces"
	"github.com/go-lynx/lynx/plugins"
)

// blockingDriver is a database/sql driver whose connections block on Ping
// until the context is done, letting tests prove that startup honors ctx.
type blockingDriver struct{}

type blockingConn struct{}

func (blockingDriver) Open(string) (driver.Conn, error) { return blockingConn{}, nil }

func (blockingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (blockingConn) Close() error                        { return nil }
func (blockingConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (blockingConn) Ping(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

var registerBlockingDriver sync.Once

func newTestSQLPlugin(t *testing.T) *SQLPlugin {
	t.Helper()
	registerBlockingDriver.Do(func() { sql.Register("lynx-blocking", blockingDriver{}) })
	cfg := &interfaces.Config{
		Driver:       "lynx-blocking",
		DSN:          "blocking://",
		MaxOpenConns: 2,
		MaxIdleConns: 1,
	}
	return NewBaseSQLPlugin("test-sql", "test.sql", "test", "v0", "lynx.test", 1, cfg)
}

func TestSQLPlugin_HasTrueContextLifecycle(t *testing.T) {
	p := newTestSQLPlugin(t)
	if !plugins.HasTrueContextLifecycle(p) {
		t.Fatal("expected SQLPlugin to report a true context lifecycle")
	}
	if !plugins.SupportsContextSteps(p) {
		t.Fatal("expected SQLPlugin to expose context-aware step hooks")
	}
	var _ plugins.ContextStartupTasker = p
	var _ plugins.ContextCleanupTasker = p
}

func TestSQLPlugin_StartupTasksContext_AlreadyCanceled(t *testing.T) {
	p := newTestSQLPlugin(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := p.StartupTasksContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("startup did not return promptly: %v", elapsed)
	}
	if p.IsConnected() {
		t.Fatal("plugin must not be connected after a canceled startup")
	}
}

func TestSQLPlugin_StartupTasksContext_CancelsInFlightConnect(t *testing.T) {
	p := newTestSQLPlugin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.StartupTasksContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("startup ignored ctx deadline: %v", elapsed)
	}
	if p.IsConnected() {
		t.Fatal("plugin must not be connected after a canceled connect")
	}
}

func TestSQLPlugin_CleanupTasksContext_AlreadyCanceled(t *testing.T) {
	p := newTestSQLPlugin(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.CleanupTasksContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if p.IsClosed() {
		t.Fatal("a canceled cleanup must not mark the plugin closed")
	}
	// Legacy path still works and closes the plugin.
	if err := p.CleanupTasks(); err != nil {
		t.Fatalf("legacy CleanupTasks failed: %v", err)
	}
	if !p.IsClosed() {
		t.Fatal("expected plugin to be closed after CleanupTasks")
	}
}
