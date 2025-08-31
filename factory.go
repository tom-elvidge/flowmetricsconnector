package flowmetricsconnector 

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

var componentType = component.MustNewType("flowmetrics")

func NewFactory() connector.Factory {
	return connector.NewFactory(
		componentType,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetricsConnector, component.StabilityLevelAlpha))
}

func createDefaultConfig() component.Config {
	return &Config{
		Timeout: 30 * time.Minute,
	}
}

func createTracesToMetricsConnector(
	ctx context.Context,
	params connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c, err := newConnector(params.TelemetrySettings, cfg)
	if err != nil {
		return nil, err
	}
	c.metricsConsumer = nextConsumer
	return c, nil
}