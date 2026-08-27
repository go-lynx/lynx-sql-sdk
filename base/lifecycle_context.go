package base

// IsContextAware asserts that the SQL plugin lifecycle genuinely observes
// context cancellation. The core BasePlugin drives StartContext/StopContext and
// routes into the StartupTasksContext / CleanupTasksContext hooks, which bind
// the connect (open/ping/verify/retry) and shutdown (stop collectors/close pool)
// work to the caller's context.
//
// Plugins that embed *SQLPlugin and wrap StartupTasks/CleanupTasks with their
// own logic must also define StartupTasksContext/CleanupTasksContext on the
// outer type (delegating here), otherwise the promoted base hooks would bypass
// the wrapper.
func (p *SQLPlugin) IsContextAware() bool {
	return true
}
