package flowmetricsconnector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type flowStart struct {
	Name          string
	StartTime     pcommon.Timestamp // OpenTelemetry timestamp
	CorrelationId string
	CreatedAt     time.Time
}

type connectorImp struct {
	config          Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	startTime       pcommon.Timestamp

	// Flow correlation state
	flowStarts    map[string]*flowStart // key: correlationId
	mutex         sync.RWMutex
	cleanupTicker *time.Ticker
	stopChan      chan struct{}

	// Cumulative metric counters by flow name
	requestTotals map[string]int64
	errorTotals   map[string]int64

	// Histogram state for latency metrics by flow name and outcome
	histogramCounts  map[string]uint64
	histogramSums    map[string]float64
	histogramBuckets map[string][]uint64
}

func NewConnector(settings component.TelemetrySettings, config component.Config) (component.Component, error) {
	return newConnector(settings, config)
}

func newConnector(settings component.TelemetrySettings, config component.Config) (*connectorImp, error) {
	cfg := config.(*Config)
	settings.Logger.Info("Building flowmetrics connector",
		zap.Duration("timeout", cfg.Timeout))

	connector := &connectorImp{
		config:           *cfg,
		logger:           settings.Logger,
		startTime:        pcommon.NewTimestampFromTime(time.Now()),
		flowStarts:       make(map[string]*flowStart),
		cleanupTicker:    time.NewTicker(5 * time.Minute),
		stopChan:         make(chan struct{}),
		requestTotals:    make(map[string]int64),
		errorTotals:      make(map[string]int64),
		histogramCounts:  make(map[string]uint64),
		histogramSums:    make(map[string]float64),
		histogramBuckets: make(map[string][]uint64),
	}

	go connector.cleanupRoutine()

	return connector, nil
}

func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type flowEvent struct {
	span          ptrace.Span
	eventName     string
	flowName      string
	boundary      string
	correlationId string
	timestamp     pcommon.Timestamp
	outcome       string // for end events
}

func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var startEvents []flowEvent
	var endEvents []flowEvent

	// Get all the start and end flow events from the spans
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				events := span.Events()

				for eventIdx := 0; eventIdx < events.Len(); eventIdx++ {
					event := events.At(eventIdx)
					eventName := event.Name()

					// Check if this is a flow event
					flowName, boundary := c.parseFlowEventName(eventName)
					if flowName == "" {
						continue
					}

					// Get flow event correlation id
					eventAttrs := event.Attributes()
					correlationId, hasCorrelation := eventAttrs.Get("correlation-id")
					if !hasCorrelation {
						c.logger.Warn("Flow event missing required attribute correlation-id",
							zap.String("event", eventName),
							zap.String("flow", flowName))
						continue
					}

					flowEvt := flowEvent{
						span:          span,
						eventName:     eventName,
						flowName:      flowName,
						boundary:      boundary,
						correlationId: correlationId.Str(),
						timestamp:     event.Timestamp(),
					}

					if boundary == "start" {
						startEvents = append(startEvents, flowEvt)
					} else if boundary == "end" {
						// Get outcome for end events
						outcomeAttr, hasOutcome := eventAttrs.Get("outcome")
						if !hasOutcome {
							c.logger.Warn("Flow end event missing required attribute outcome",
								zap.String("event", eventName),
								zap.String("flow", flowName),
								zap.String("correlation_id", flowEvt.correlationId))
							continue
						}

						outcome := outcomeAttr.Str()
						if outcome != "success" && outcome != "failure" {
							c.logger.Warn("Flow end event has invalid value for outcome attribute",
								zap.String("event", eventName),
								zap.String("flow", flowName),
								zap.String("correlation_id", flowEvt.correlationId),
								zap.String("outcome", outcome))
							continue
						}

						flowEvt.outcome = outcome
						endEvents = append(endEvents, flowEvt)
					}
				}
			}
		}
	}

	// Handle all the flow start events
	for _, flowEvt := range startEvents {
		c.handleFlowStart(ctx, flowEvt.flowName, flowEvt.correlationId, flowEvt.timestamp)
	}

	// Handle all the flow end events
	for _, flowEvt := range endEvents {
		c.handleFlowEnd(ctx, flowEvt.span, flowEvt.flowName, flowEvt.correlationId, flowEvt.timestamp, flowEvt.outcome)
	}

	return nil
}

func (c *connectorImp) parseFlowEventName(eventName string) (flowName string, boundary string) {
	// "flow.{flowName}.start" or "flow.{flowName}.end"
	if !strings.HasPrefix(eventName, "flow.") {
		return "", ""
	}

	parts := strings.Split(eventName, ".")
	if len(parts) != 3 {
		return "", ""
	}

	if parts[0] != "flow" {
		return "", ""
	}

	flowName = parts[1]
	boundary = parts[2]

	if boundary != "start" && boundary != "end" {
		return "", ""
	}

	return flowName, boundary
}

func (c *connectorImp) handleFlowStart(ctx context.Context, flowName, correlationId string, eventTimestamp pcommon.Timestamp) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.flowStarts[correlationId] = &flowStart{
		Name:          flowName,
		StartTime:     eventTimestamp,
		CorrelationId: correlationId,
		CreatedAt:     time.Now(),
	}

	// Flow total metrics
	if err := c.sendRequestTotalMetric(ctx, flowName, correlationId); err != nil {
		c.logger.Error("Failed to send flow total metric", zap.Error(err))
	}
}

func (c *connectorImp) handleFlowEnd(ctx context.Context, span ptrace.Span, flowName, correlationId string, eventTimestamp pcommon.Timestamp, outcome string) {
	c.mutex.Lock()
	flowStart, exists := c.flowStarts[correlationId]
	if exists {
		delete(c.flowStarts, correlationId)
	}

	c.mutex.Unlock()

	if !exists {
		c.logger.Warn("Flow end event without correlated flow start event",
			zap.String("flow", flowName),
			zap.String("correlation_id", correlationId))
		return
	}

	durationNs := int64(eventTimestamp) - int64(flowStart.StartTime)
	durationSeconds := float64(durationNs) / 1e9

	isFailure := outcome == "failure"

	// Flow latency metric
	if err := c.sendLatencyMetric(ctx, flowName, correlationId, durationSeconds, outcome); err != nil {
		c.logger.Error("Failed to send flow latency metric", zap.Error(err))
	}

	// Flow error metric
	if isFailure {
		if err := c.sendErrorMetric(ctx, flowName, correlationId); err != nil {
			c.logger.Error("Failed to send flow error metric", zap.Error(err))
		}
	}
}

func (c *connectorImp) sendRequestTotalMetric(ctx context.Context, flowName, correlationId string) error {
	c.requestTotals[flowName]++
	currentTotal := c.requestTotals[flowName]

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

	scope := scopeMetrics.Scope()
	scope.SetName("flowmetrics")
	scope.SetVersion("1.0.0")

	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName(fmt.Sprintf("flow_%s_total", flowName))
	metric.SetDescription(fmt.Sprintf("Total number of %s flows", flowName))
	metric.SetUnit("1")

	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)

	dataPoint := sum.DataPoints().AppendEmpty()
	dataPoint.SetIntValue(currentTotal)
	dataPoint.SetStartTimestamp(c.startTime)
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	attrs := dataPoint.Attributes()
	attrs.PutStr("flow_name", flowName)

	return c.metricsConsumer.ConsumeMetrics(ctx, metrics)
}

func (c *connectorImp) sendLatencyMetric(ctx context.Context, flowName, correlationId string, durationSeconds float64, outcome string) error {
	// Add validation for negative durations
	if durationSeconds < 0 {
		c.logger.Warn("Negative duration detected, likely due to out-of-order events",
			zap.String("flow", flowName),
			zap.String("correlation_id", correlationId),
			zap.Float64("duration", durationSeconds))
		return nil // Skip this observation
	}

	// Create unique key for this flow and outcome combination
	histKey := fmt.Sprintf("%s_%s", flowName, outcome)

	c.histogramCounts[histKey]++
	c.histogramSums[histKey] += durationSeconds

	buckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	if c.histogramBuckets[histKey] == nil {
		c.histogramBuckets[histKey] = make([]uint64, len(buckets)+1)
	}

	// Update bucket counts for this observation (OpenTelemetry format - increment only one bucket)
	bucketFound := false
	for i, bound := range buckets {
		if durationSeconds <= bound {
			c.histogramBuckets[histKey][i]++
			bucketFound = true
			break // Only increment the first matching bucket
		}
	}

	// If no bucket matched, increment the overflow bucket (+Inf)
	if !bucketFound {
		c.histogramBuckets[histKey][len(buckets)]++
	}

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

	scope := scopeMetrics.Scope()
	scope.SetName("flowmetrics")
	scope.SetVersion("1.0.0")

	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName(fmt.Sprintf("flow_%s_latency", flowName))
	metric.SetDescription(fmt.Sprintf("Latency of %s flow in seconds", flowName))
	metric.SetUnit("s")

	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dataPoint := histogram.DataPoints().AppendEmpty()
	dataPoint.SetCount(c.histogramCounts[histKey])
	dataPoint.SetSum(c.histogramSums[histKey])
	dataPoint.SetStartTimestamp(c.startTime)
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Set histogram buckets
	dataPoint.BucketCounts().EnsureCapacity(len(buckets) + 1)
	dataPoint.ExplicitBounds().EnsureCapacity(len(buckets))

	for _, bound := range buckets {
		dataPoint.ExplicitBounds().Append(bound)
	}

	for _, bucketCount := range c.histogramBuckets[histKey] {
		dataPoint.BucketCounts().Append(bucketCount)
	}

	attrs := dataPoint.Attributes()
	attrs.PutStr("flow_name", flowName)
	attrs.PutStr("outcome", outcome)

	return c.metricsConsumer.ConsumeMetrics(ctx, metrics)
}

func (c *connectorImp) sendErrorMetric(ctx context.Context, flowName, correlationId string) error {
	c.errorTotals[flowName]++
	currentTotal := c.errorTotals[flowName]

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

	scope := scopeMetrics.Scope()
	scope.SetName("flowmetrics")
	scope.SetVersion("1.0.0")

	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName(fmt.Sprintf("flow_%s_errors", flowName))
	metric.SetDescription(fmt.Sprintf("Total number of %s flow errors", flowName))
	metric.SetUnit("1")

	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)

	dataPoint := sum.DataPoints().AppendEmpty()
	dataPoint.SetIntValue(currentTotal)
	dataPoint.SetStartTimestamp(c.startTime)
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	attrs := dataPoint.Attributes()
	attrs.PutStr("flow_name", flowName)

	return c.metricsConsumer.ConsumeMetrics(ctx, metrics)
}

func (c *connectorImp) cleanupRoutine() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.cleanup()
		case <-c.stopChan:
			return
		}
	}
}

func (c *connectorImp) cleanup() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	cutoff := time.Now().Add(-c.config.Timeout)
	var toDelete []string

	for correlationId, flowStart := range c.flowStarts {
		if flowStart.CreatedAt.Before(cutoff) {
			toDelete = append(toDelete, correlationId)
		}
	}

	for _, correlationId := range toDelete {
		flowStart := c.flowStarts[correlationId]
		c.logger.Warn("Cleaning up stale flow",
			zap.String("flow", flowStart.Name),
			zap.String("correlation_id", correlationId),
			zap.Duration("age", time.Since(flowStart.CreatedAt)))
		delete(c.flowStarts, correlationId)
	}

	if len(toDelete) > 0 {
		c.logger.Info("Cleaned up stale flows", zap.Int("count", len(toDelete)))
	}
}

func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	c.logger.Info("Starting flowmetrics connector")
	return nil
}

func (c *connectorImp) Shutdown(ctx context.Context) error {
	c.logger.Info("Shutting down flowmetrics connector")

	close(c.stopChan)
	c.cleanupTicker.Stop()

	c.mutex.RLock()
	c.mutex.RUnlock()

	return nil
}
