# SQL SDK for Lynx

`lynx-sql-sdk` is the shared SQL runtime layer used by concrete database plugins such as `lynx-mysql`, `lynx-pgsql`, and `lynx-mssql`.

It is not a standalone Lynx runtime plugin and it does not own a separate `lynx.sql` YAML block.

## What this repo owns

- shared interfaces in [`interfaces`](./interfaces)
- the reusable base runtime in [`base.SQLPlugin`](./base)
- connection validation before handout
- retry and reconnect behavior
- health checks, pool monitoring, slow-query monitoring, and leak detection
- idempotent cleanup and resource shutdown semantics

## What this repo does not own

- no `lynx.sql` config prefix
- no independent plugin registration for applications to load directly
- no standalone application-facing getters such as `GetHealthChecker()` or `GetMetricsCollector()`
- no datasource selection outside the concrete MySQL / PostgreSQL / MSSQL plugins

## Configuration ownership

Configure the concrete plugin you actually run:

- MySQL: `lynx.mysql`
- PostgreSQL: `lynx.pgsql`
- MSSQL: `lynx.mssql`

The shared config fields consumed by `base.SQLPlugin` are defined in [`interfaces/sql.go`](./interfaces/sql.go), but those fields must live under the concrete plugin's YAML root.

## Runtime guidance

- Do not cache a long-lived `*sql.DB` handle when auto-reconnect is enabled. A reconnect replaces the pool; stale cached handles can become closed.
- Prefer resolving the current DB handle close to the point of use, or wrap the concrete plugin behind an interface similar to `interfaces.DBProvider`.
- Repeated `CleanupTasks()` calls are safe and return `nil`.

## Related repos

- `lynx-mysql`
- `lynx-pgsql`
- `lynx-mssql`

Those repos own the runnable YAML examples and the application-facing plugin access patterns.
