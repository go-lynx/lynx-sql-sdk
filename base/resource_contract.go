package base

import (
	"github.com/go-lynx/lynx-sql-sdk/interfaces"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
)

const (
	sqlSharedProviderSuffix = ".provider"
	sqlPrivateDBName        = "db"
	sqlPrivateConfigName    = "config"
	sqlPrivateDialectName   = "dialect"
)

// SetProvider sets the stable DB provider published into runtime resources.
func (p *SQLPlugin) SetProvider(provider interfaces.DBProvider) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.provider = provider
}

func (p *SQLPlugin) bindRuntime(rt plugins.Runtime) {
	if rt == nil {
		return
	}
	p.mu.Lock()
	p.runtime = rt.WithPluginContext(p.Name())
	p.mu.Unlock()
}

func (p *SQLPlugin) publishResourceContract() {
	p.mu.RLock()
	rt := p.runtime
	provider := p.provider
	db := p.db
	cfg := p.config
	dialect := p.dialect
	name := p.Name()
	p.mu.RUnlock()

	if rt == nil {
		return
	}
	if provider == nil {
		log.Warnf("sql plugin %s has no provider; shared provider resource will not be published", name)
	} else {
		for _, resourceName := range []string{name, name + sqlSharedProviderSuffix} {
			if err := rt.RegisterSharedResource(resourceName, provider); err != nil {
				log.Warnf("failed to register sql shared resource %s: %v", resourceName, err)
			}
		}
		if err := rt.RegisterPrivateResource("provider", provider); err != nil {
			log.Warnf("failed to register sql private provider resource: %v", err)
		}
	}

	if db != nil {
		if err := rt.RegisterPrivateResource(sqlPrivateDBName, db); err != nil {
			log.Warnf("failed to register sql private db resource: %v", err)
		}
	}
	if cfg != nil {
		if err := rt.RegisterPrivateResource(sqlPrivateConfigName, cfg); err != nil {
			log.Warnf("failed to register sql private config resource: %v", err)
		}
	}
	if dialect != "" {
		if err := rt.RegisterPrivateResource(sqlPrivateDialectName, dialect); err != nil {
			log.Warnf("failed to register sql private dialect resource: %v", err)
		}
	}
}

// SharedProviderResourceName returns the canonical shared resource name for a SQL provider.
func (p *SQLPlugin) SharedProviderResourceName() string {
	if p == nil {
		return ""
	}
	return p.Name() + sqlSharedProviderSuffix
}
