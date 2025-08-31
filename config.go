package flowmetricsconnector 

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Timeout time.Duration `mapstructure:"timeout"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Minute
	}
	return nil
}