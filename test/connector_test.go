package flowmetricsconnector_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tom-elvidge/flowmetricsconnector"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestNewConnector(t *testing.T) {
	factory := flowmetricsconnector.NewFactory()
	cfg := factory.CreateDefaultConfig()

	assert.NotNil(t, factory)
	assert.NotNil(t, cfg)
}

func TestConsumeTracesWithStartedFlow(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)

	// Create test traces with flow events
	traces := createTestTracesWithStartEvent()

	// Consume traces
	ctx := context.Background()
	err := tracesConnector.ConsumeTraces(ctx, traces)
	require.NoError(t, err)

	// Verify metrics were generated
	assert.Greater(t, len(mockConsumer.AllMetrics()), 0)

	// Check that we have the expected metric types
	allMetrics := mockConsumer.AllMetrics()
	var hasRequestTotal, hasLatency bool

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()
					if name == "flow_order_processing_total" {
						hasRequestTotal = true
					}
					if name == "flow_order_processing_latency" {
						hasLatency = true
					}
				}
			}
		}
	}

	assert.True(t, hasRequestTotal, "Should have request total metric")
	assert.False(t, hasLatency, "Should not yet have latency metric")
}

func TestConsumeTracesWithCompleteFlow(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	// Create and consume traces with start event
	startTraces := createTestTracesWithStartEvent()
	err := tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	// Create and consume traces with end event
	endTraces := createTestTracesWithEndEvent("success")
	err = tracesConnector.ConsumeTraces(ctx, endTraces)
	require.NoError(t, err)

	// Verify metrics were generated (start + end metrics)
	assert.Greater(t, len(mockConsumer.AllMetrics()), 1)

	// Check that we have the expected metric types
	allMetrics := mockConsumer.AllMetrics()
	var hasRequestTotal, hasLatency bool

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()
					if name == "flow_order_processing_total" {
						hasRequestTotal = true
					}
					if name == "flow_order_processing_latency" {
						hasLatency = true
					}
				}
			}
		}
	}

	assert.True(t, hasRequestTotal, "Should have request total metric")
	assert.True(t, hasLatency, "Should have latency metric")
}

func TestConsumeTracesWithFailureOutcome(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	// Create and consume traces with start event
	startTraces := createTestTracesWithStartEvent()
	err := tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	// Create and consume traces with failure end event
	endTraces := createTestTracesWithEndEvent("failure")
	err = tracesConnector.ConsumeTraces(ctx, endTraces)
	require.NoError(t, err)

	// Verify metrics were generated including error metric
	assert.Greater(t, len(mockConsumer.AllMetrics()), 1)
}

func TestConsumeTracesEndEventWithoutStart(t *testing.T) {
	tracesConnector, _ := createTestConnector(t)
	ctx := context.Background()

	// Create and consume traces with only end event (no start)
	endTraces := createTestTracesWithEndEvent("success")
	err := tracesConnector.ConsumeTraces(ctx, endTraces)
	require.NoError(t, err)

	// Should handle gracefully without errors
}

func TestConsumeTracesWithKnownLatency(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	// Test different latency values and their expected bucket placement
	testCases := []struct {
		name           string
		latencySeconds float64
		expectedBucket int // Index of the bucket that should be incremented
		correlationId  string
		flowName       string
	}{
		{
			name:           "Very fast flow (1ms)",
			latencySeconds: 0.001, // 1ms - should go in bucket 0 (≤ 0.005)
			expectedBucket: 0,
			correlationId:  "test-fast",
			flowName:       "fast_processing",
		},
		{
			name:           "Medium flow (50ms)",
			latencySeconds: 0.05, // 50ms - should go in bucket 3 (≤ 0.05)
			expectedBucket: 3,
			correlationId:  "test-medium",
			flowName:       "medium_processing",
		},
		{
			name:           "Slow flow (2.5s)",
			latencySeconds: 2.5, // 2.5s - should go in bucket 8 (≤ 2.5)
			expectedBucket: 8,
			correlationId:  "test-slow",
			flowName:       "slow_processing",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear previous metrics
			mockConsumer.Reset()

			// Create start trace with known timestamp
			baseTime := time.Now()
			startTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", tc.flowName), tc.correlationId, baseTime)

			// Create end trace with calculated timestamp for desired latency
			endTime := baseTime.Add(time.Duration(tc.latencySeconds * float64(time.Second)))
			endTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.end", tc.flowName), tc.correlationId, endTime)

			// Consume start event
			err := tracesConnector.ConsumeTraces(ctx, startTraces)
			require.NoError(t, err)

			// Consume end event
			err = tracesConnector.ConsumeTraces(ctx, endTraces)
			require.NoError(t, err)

			// Verify metrics were generated
			allMetrics := mockConsumer.AllMetrics()
			require.Greater(t, len(allMetrics), 0, "Should have generated metrics")

			// Find the latency histogram metric
			var latencyMetric pmetric.Metric
			var found bool
			expectedMetricName := fmt.Sprintf("flow_%s_latency", tc.flowName)
			for _, metrics := range allMetrics {
				for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
					rm := metrics.ResourceMetrics().At(i)
					for j := 0; j < rm.ScopeMetrics().Len(); j++ {
						sm := rm.ScopeMetrics().At(j)
						for k := 0; k < sm.Metrics().Len(); k++ {
							metric := sm.Metrics().At(k)
							if metric.Name() == expectedMetricName {
								latencyMetric = metric
								found = true
								break
							}
						}
					}
				}
			}
			require.True(t, found, "Should have found latency histogram metric")

			// Verify histogram properties
			histogram := latencyMetric.Histogram()
			require.Equal(t, 1, histogram.DataPoints().Len(), "Should have exactly one data point")

			dataPoint := histogram.DataPoints().At(0)
			assert.Equal(t, uint64(1), dataPoint.Count(), "Should have count of 1")
			assert.InDelta(t, tc.latencySeconds, dataPoint.Sum(), 0.001, "Sum should match expected latency")

			// Verify bucket counts
			// The current implementation uses OpenTelemetry format - only the specific bucket gets incremented
			// Buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
			bucketCounts := dataPoint.BucketCounts()
			require.Greater(t, bucketCounts.Len(), tc.expectedBucket, "Should have enough buckets")

			// Define the bucket boundaries for reference
			buckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

			// Validate bucket counts for OpenTelemetry individual bucket behavior
			for i, bound := range buckets {
				if tc.latencySeconds <= bound && i == tc.expectedBucket {
					// Only the first matching bucket should have count 1
					assert.Equal(t, uint64(1), bucketCounts.At(i),
						"Bucket %d (≤%.3fs) should have count 1 for latency %.3fs", i, bound, tc.latencySeconds)
				} else {
					// All other buckets should have count 0
					assert.Equal(t, uint64(0), bucketCounts.At(i),
						"Bucket %d (≤%.3fs) should have count 0 for latency %.3fs", i, bound, tc.latencySeconds)
				}
			}

			// The overflow bucket should have count 0 unless no other bucket matched
			overflowBucket := bucketCounts.Len() - 1
			expectedOverflowCount := uint64(0)
			if tc.latencySeconds > buckets[len(buckets)-1] {
				expectedOverflowCount = 1
			}
			assert.Equal(t, expectedOverflowCount, bucketCounts.At(overflowBucket),
				"Overflow bucket should have count %d for latency %.3fs", expectedOverflowCount, tc.latencySeconds)
		})
	}
}

func TestConsumeTracesWithNegativeDuration(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	// Create traces where end event comes before start event (negative duration)
	baseTime := time.Now()

	// Start event at a later time
	startTraces := createTestTracesWithTimestamp("flow.test_flow.start", "negative-test", baseTime.Add(time.Second))

	// End event at an earlier time (creates negative duration)
	endTraces := createTestTracesWithTimestamp("flow.test_flow.end", "negative-test", baseTime)

	// Consume start event
	err := tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	// Consume end event (should be skipped due to negative duration)
	err = tracesConnector.ConsumeTraces(ctx, endTraces)
	require.NoError(t, err)

	// Verify that no latency metric was generated due to negative duration
	allMetrics := mockConsumer.AllMetrics()
	hasLatencyMetric := false

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					if strings.Contains(metric.Name(), "_latency") {
						hasLatencyMetric = true
					}
				}
			}
		}
	}

	assert.False(t, hasLatencyMetric, "Should not generate latency metric for negative duration")
}

func TestConsumeTracesInvalidEvents(t *testing.T) {
	tracesConnector, _ := createTestConnector(t)
	ctx := context.Background()

	// Test various invalid events
	invalidTraces := createTestTracesWithInvalidEvents()
	err := tracesConnector.ConsumeTraces(ctx, invalidTraces)
	require.NoError(t, err)

	// Should handle invalid events gracefully
}

func TestMultipleFlowsWithSameCorrelationId(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	correlationId := "shared-correlation-123"

	// Start two different flows with the same correlation ID
	flow1StartTraces := createTestTracesWithTimestamp("flow.order_processing.start", correlationId, time.Now())
	flow2StartTraces := createTestTracesWithTimestamp("flow.payment_processing.start", correlationId, time.Now().Add(10*time.Millisecond))

	// End both flows
	flow1EndTraces := createTestTracesWithTimestamp("flow.order_processing.end", correlationId, time.Now().Add(100*time.Millisecond))
	flow2EndTraces := createTestTracesWithTimestamp("flow.payment_processing.end", correlationId, time.Now().Add(200*time.Millisecond))

	// Consume all events
	err := tracesConnector.ConsumeTraces(ctx, flow1StartTraces)
	require.NoError(t, err)

	err = tracesConnector.ConsumeTraces(ctx, flow2StartTraces)
	require.NoError(t, err)

	err = tracesConnector.ConsumeTraces(ctx, flow1EndTraces)
	require.NoError(t, err)

	err = tracesConnector.ConsumeTraces(ctx, flow2EndTraces)
	require.NoError(t, err)

	// Check metrics - we should have metrics for both flows
	allMetrics := mockConsumer.AllMetrics()
	var hasOrderTotal, hasPaymentTotal, hasOrderLatency, hasPaymentLatency bool

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()

					if name == "flow_order_processing_total" {
						hasOrderTotal = true
					}
					if name == "flow_payment_processing_total" {
						hasPaymentTotal = true
					}
					if name == "flow_order_processing_latency" {
						hasOrderLatency = true
					}
					if name == "flow_payment_processing_latency" {
						hasPaymentLatency = true
					}
				}
			}
		}
	}

	// Both flows should have their respective metrics
	assert.True(t, hasOrderTotal, "Should have order processing total metric")
	assert.True(t, hasPaymentTotal, "Should have payment processing total metric")
	assert.True(t, hasOrderLatency, "Should have order processing latency metric")
	assert.True(t, hasPaymentLatency, "Should have payment processing latency metric")
}

func TestSameFlowWithDifferentCorrelationIds(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	flowName := "order_processing"
	correlationId1 := "correlation-123"
	correlationId2 := "correlation-456"

	// Start same flow type with different correlation IDs
	flow1StartTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", flowName), correlationId1, time.Now())
	flow2StartTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", flowName), correlationId2, time.Now().Add(10*time.Millisecond))

	// End both flows
	flow1EndTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.end", flowName), correlationId1, time.Now().Add(100*time.Millisecond))
	flow2EndTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.end", flowName), correlationId2, time.Now().Add(200*time.Millisecond))

	// Consume all events
	err := tracesConnector.ConsumeTraces(ctx, flow1StartTraces)
	require.NoError(t, err)

	err = tracesConnector.ConsumeTraces(ctx, flow2StartTraces)
	require.NoError(t, err)

	err = tracesConnector.ConsumeTraces(ctx, flow1EndTraces)
	require.NoError(t, err)

	err = tracesConnector.ConsumeTraces(ctx, flow2EndTraces)
	require.NoError(t, err)

	// Check metrics - we should have total count of 2 for the flow
	allMetrics := mockConsumer.AllMetrics()
	var totalCount int64 = 0
	var latencyMetricCount int = 0

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()

					if name == fmt.Sprintf("flow_%s_total", flowName) {
						// Get the latest total count
						if metric.Type() == pmetric.MetricTypeSum {
							for dp := 0; dp < metric.Sum().DataPoints().Len(); dp++ {
								dataPoint := metric.Sum().DataPoints().At(dp)
								if dataPoint.IntValue() > totalCount {
									totalCount = dataPoint.IntValue()
								}
							}
						}
					}
					if name == fmt.Sprintf("flow_%s_latency", flowName) {
						latencyMetricCount++
					}
				}
			}
		}
	}

	// Both flow instances should be counted
	assert.Equal(t, int64(2), totalCount, "Should have total count of 2 for the flow")
	assert.Equal(t, 2, latencyMetricCount, "Should have 2 latency metrics for the 2 flow instances")
}

// Helper functions to create test data

func createDefaultConfig() *flowmetricsconnector.Config {
	return &flowmetricsconnector.Config{
		Timeout: 30 * time.Minute,
	}
}

// Helper functions to create test data

func createTestConnector(t *testing.T) (connector.Traces, *consumertest.MetricsSink) {
	factory := flowmetricsconnector.NewFactory()
	cfg := factory.CreateDefaultConfig()

	settings := connector.Settings{
		ID:                component.NewID(component.MustNewType("flowmetrics")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}

	mockConsumer := &consumertest.MetricsSink{}

	conn, err := factory.CreateTracesToMetrics(context.Background(), settings, cfg, mockConsumer)
	require.NoError(t, err)

	return conn.(connector.Traces), mockConsumer
}

func createTestTracesWithTimestamp(eventName, correlationId string, timestamp time.Time) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
	span := scopeSpan.Spans().AppendEmpty()

	span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("test-span-timestamp")

	// Add flow event with specific timestamp
	event := span.Events().AppendEmpty()
	event.SetName(eventName)
	event.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	attrs := event.Attributes()
	attrs.PutStr("correlation-id", correlationId)

	// If this is an end event, add the outcome attribute
	if strings.Contains(eventName, ".end") {
		attrs.PutStr("outcome", "success")
	}

	return traces
}

func createTestTracesWithStartEvent() ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
	span := scopeSpan.Spans().AppendEmpty()

	span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("test-span-start")

	// Add flow start event
	event := span.Events().AppendEmpty()
	event.SetName("flow.order_processing.start")
	event.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	attrs := event.Attributes()
	attrs.PutStr("correlation-id", "test-correlation-456")

	return traces
}

func createTestTracesWithEndEvent(outcome string) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
	span := scopeSpan.Spans().AppendEmpty()

	span.SetTraceID([16]byte{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{2, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("test-span-end")

	// Add flow end event
	event := span.Events().AppendEmpty()
	event.SetName("flow.order_processing.end")
	event.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(100 * time.Millisecond)))
	attrs := event.Attributes()
	attrs.PutStr("correlation-id", "test-correlation-456")
	attrs.PutStr("outcome", outcome)

	return traces
}

func createTestTracesWithInvalidEvents() ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
	span := scopeSpan.Spans().AppendEmpty()

	span.SetTraceID([16]byte{3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{3, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("test-span-invalid")

	// Event without correlation-id
	event1 := span.Events().AppendEmpty()
	event1.SetName("flow.order_processing.start")
	event1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Event with invalid outcome
	event2 := span.Events().AppendEmpty()
	event2.SetName("flow.order_processing.end")
	event2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	attrs2 := event2.Attributes()
	attrs2.PutStr("correlation-id", "test-correlation-invalid")
	attrs2.PutStr("outcome", "invalid-outcome")

	// Non-flow event
	event3 := span.Events().AppendEmpty()
	event3.SetName("regular.event")
	event3.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return traces
}

func TestIncompleteFlowsRecordedAsErrors(t *testing.T) {
	// Create connector with a very short timeout for testing
	factory := flowmetricsconnector.NewFactory()
	cfg := factory.CreateDefaultConfig().(*flowmetricsconnector.Config)
	cfg.Timeout = 50 * time.Millisecond // Very short timeout for testing

	settings := connector.Settings{
		ID:                component.NewID(component.MustNewType("flowmetrics")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}

	mockConsumer := &consumertest.MetricsSink{}
	tracesConnector, err := factory.CreateTracesToMetrics(context.Background(), settings, cfg, mockConsumer)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a flow start event
	correlationId := "incomplete-flow-123"
	flowName := "test_processing"
	startTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", flowName), correlationId, time.Now())

	// Consume start event
	err = tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	// Clear existing metrics to focus on cleanup-generated metrics
	mockConsumer.Reset()

	// Wait for the timeout period to pass
	time.Sleep(100 * time.Millisecond)

	// Manually trigger cleanup using the test helper
	if connectorImpl, ok := tracesConnector.(interface{ ForceCleanup(context.Context) }); ok {
		connectorImpl.ForceCleanup(ctx)
	} else {
		t.Fatal("Cannot access ForceCleanup method")
	}

	allMetrics := mockConsumer.AllMetrics()
	var hasErrorMetric bool
	var errorReason string
	var errorMetricValue int64

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()
					if name == fmt.Sprintf("flow_%s_errors", flowName) {
						hasErrorMetric = true
						if metric.Type() == pmetric.MetricTypeSum {
							for dp := 0; dp < metric.Sum().DataPoints().Len(); dp++ {
								dataPoint := metric.Sum().DataPoints().At(dp)
								errorMetricValue = dataPoint.IntValue()
								// Check for error_reason attribute
								if val, exists := dataPoint.Attributes().Get("error_reason"); exists {
									errorReason = val.Str()
								}
							}
						}
					}
				}
			}
		}
	}

	assert.True(t, hasErrorMetric, "Should have error metric for incomplete flow")
	assert.Equal(t, int64(1), errorMetricValue, "Should have 1 error recorded for incomplete flow")
	assert.Equal(t, "incomplete", errorReason, "Error metric should have 'incomplete' as error_reason")
}

func TestFailedFlowsHaveCorrectErrorReason(t *testing.T) {
	tracesConnector, mockConsumer := createTestConnector(t)
	ctx := context.Background()

	correlationId := "failed-flow-456"
	flowName := "payment_processing"

	// Create and consume traces with start event
	startTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", flowName), correlationId, time.Now())
	err := tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	// Create and consume traces with failure end event
	endTraces := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.end", flowName), correlationId, time.Now().Add(100*time.Millisecond))
	// Add the outcome attribute to the end event
	resourceSpan := endTraces.ResourceSpans().At(0)
	scopeSpan := resourceSpan.ScopeSpans().At(0)
	span := scopeSpan.Spans().At(0)
	event := span.Events().At(0)
	attrs := event.Attributes()
	attrs.PutStr("outcome", "failure")

	err = tracesConnector.ConsumeTraces(ctx, endTraces)
	require.NoError(t, err)

	// Check that error metric has error_reason="failure"
	allMetrics := mockConsumer.AllMetrics()
	var hasErrorMetric bool
	var errorReason string

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()
					if name == fmt.Sprintf("flow_%s_errors", flowName) {
						hasErrorMetric = true
						if metric.Type() == pmetric.MetricTypeSum {
							for dp := 0; dp < metric.Sum().DataPoints().Len(); dp++ {
								dataPoint := metric.Sum().DataPoints().At(dp)
								// Check for error_reason attribute
								if val, exists := dataPoint.Attributes().Get("error_reason"); exists {
									errorReason = val.Str()
								}
							}
						}
					}
				}
			}
		}
	}

	assert.True(t, hasErrorMetric, "Should have error metric for failed flow")
	assert.Equal(t, "failure", errorReason, "Error metric should have 'failure' as error_reason")
}

func TestErrorCountingWithMixedReasons(t *testing.T) {
	// Create connector with short timeout
	factory := flowmetricsconnector.NewFactory()
	cfg := factory.CreateDefaultConfig().(*flowmetricsconnector.Config)
	cfg.Timeout = 50 * time.Millisecond

	settings := connector.Settings{
		ID:                component.NewID(component.MustNewType("flowmetrics")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}

	mockConsumer := &consumertest.MetricsSink{}
	tracesConnector, err := factory.CreateTracesToMetrics(context.Background(), settings, cfg, mockConsumer)
	require.NoError(t, err)

	ctx := context.Background()
	flowName := "mixed_processing"

	// First: Create a failed flow (explicit failure)
	failedCorrelationId := "failed-123"
	startTraces1 := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", flowName), failedCorrelationId, time.Now())
	err = tracesConnector.ConsumeTraces(ctx, startTraces1)
	require.NoError(t, err)

	endTraces1 := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.end", flowName), failedCorrelationId, time.Now().Add(50*time.Millisecond))
	// Add failure outcome
	resourceSpan1 := endTraces1.ResourceSpans().At(0)
	scopeSpan1 := resourceSpan1.ScopeSpans().At(0)
	span1 := scopeSpan1.Spans().At(0)
	event1 := span1.Events().At(0)
	attrs1 := event1.Attributes()
	attrs1.PutStr("outcome", "failure")

	err = tracesConnector.ConsumeTraces(ctx, endTraces1)
	require.NoError(t, err)

	// Second: Create an incomplete flow (timeout)
	incompleteCorrelationId := "incomplete-456"
	startTraces2 := createTestTracesWithTimestamp(fmt.Sprintf("flow.%s.start", flowName), incompleteCorrelationId, time.Now())
	err = tracesConnector.ConsumeTraces(ctx, startTraces2)
	require.NoError(t, err)

	// Wait for timeout and force cleanup
	time.Sleep(100 * time.Millisecond)
	if connectorImpl, ok := tracesConnector.(interface{ ForceCleanup(context.Context) }); ok {
		connectorImpl.ForceCleanup(ctx)
	}

	// Check all metrics after cleanup
	allMetricsAfterCleanup := mockConsumer.AllMetrics()
	var failureCount, incompleteCount int64
	var foundFailure, foundIncomplete bool

	for _, metrics := range allMetricsAfterCleanup {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()
					if name == fmt.Sprintf("flow_%s_errors", flowName) {
						if metric.Type() == pmetric.MetricTypeSum {
							for dp := 0; dp < metric.Sum().DataPoints().Len(); dp++ {
								dataPoint := metric.Sum().DataPoints().At(dp)
								if val, exists := dataPoint.Attributes().Get("error_reason"); exists {
									reason := val.Str()
									if reason == "failure" {
										foundFailure = true
										failureCount = dataPoint.IntValue()
									} else if reason == "incomplete" {
										foundIncomplete = true
										incompleteCount = dataPoint.IntValue()
									}
								}
							}
						}
					}
				}
			}
		}
	}

	assert.True(t, foundFailure, "Should have failure error metric")
	assert.True(t, foundIncomplete, "Should have incomplete error metric")
	assert.Equal(t, int64(1), failureCount, "Should have exactly 1 failure")
	assert.Equal(t, int64(1), incompleteCount, "Should have exactly 1 incomplete")
}

func TestErrorCountingSeparationByFlowAndReason(t *testing.T) {
	// Create connector with short timeout
	factory := flowmetricsconnector.NewFactory()
	cfg := factory.CreateDefaultConfig().(*flowmetricsconnector.Config)
	cfg.Timeout = 50 * time.Millisecond

	settings := connector.Settings{
		ID:                component.NewID(component.MustNewType("flowmetrics")),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}

	mockConsumer := &consumertest.MetricsSink{}
	tracesConnector, err := factory.CreateTracesToMetrics(context.Background(), settings, cfg, mockConsumer)
	require.NoError(t, err)

	ctx := context.Background()

	// Test scenario:
	// - Flow A: 2 failures, 1 incomplete
	// - Flow B: 1 failure, 2 incompletes
	// Expected error metrics:
	// - flow_flowA_errors{error_reason="failure"} = 2
	// - flow_flowA_errors{error_reason="incomplete"} = 1
	// - flow_flowB_errors{error_reason="failure"} = 1
	// - flow_flowB_errors{error_reason="incomplete"} = 2

	// Flow A: 2 failures
	for i := 0; i < 2; i++ {
		correlationId := fmt.Sprintf("flowA-fail-%d", i)
		startTraces := createTestTracesWithTimestamp("flow.flowA.start", correlationId, time.Now())
		err = tracesConnector.ConsumeTraces(ctx, startTraces)
		require.NoError(t, err)

		endTraces := createTestTracesWithTimestamp("flow.flowA.end", correlationId, time.Now().Add(10*time.Millisecond))
		resourceSpan := endTraces.ResourceSpans().At(0)
		scopeSpan := resourceSpan.ScopeSpans().At(0)
		span := scopeSpan.Spans().At(0)
		event := span.Events().At(0)
		attrs := event.Attributes()
		attrs.PutStr("outcome", "failure")

		err = tracesConnector.ConsumeTraces(ctx, endTraces)
		require.NoError(t, err)
	}

	// Flow A: 1 incomplete
	correlationId := "flowA-incomplete"
	startTraces := createTestTracesWithTimestamp("flow.flowA.start", correlationId, time.Now())
	err = tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	// Flow B: 1 failure
	correlationId = "flowB-fail"
	startTraces = createTestTracesWithTimestamp("flow.flowB.start", correlationId, time.Now())
	err = tracesConnector.ConsumeTraces(ctx, startTraces)
	require.NoError(t, err)

	endTraces := createTestTracesWithTimestamp("flow.flowB.end", correlationId, time.Now().Add(10*time.Millisecond))
	resourceSpan := endTraces.ResourceSpans().At(0)
	scopeSpan := resourceSpan.ScopeSpans().At(0)
	span := scopeSpan.Spans().At(0)
	event := span.Events().At(0)
	attrs := event.Attributes()
	attrs.PutStr("outcome", "failure")

	err = tracesConnector.ConsumeTraces(ctx, endTraces)
	require.NoError(t, err)

	// Flow B: 2 incompletes
	for i := 0; i < 2; i++ {
		correlationId := fmt.Sprintf("flowB-incomplete-%d", i)
		startTraces := createTestTracesWithTimestamp("flow.flowB.start", correlationId, time.Now())
		err = tracesConnector.ConsumeTraces(ctx, startTraces)
		require.NoError(t, err)
	}

	// Wait and trigger cleanup for incompletes
	time.Sleep(100 * time.Millisecond)
	if connectorImpl, ok := tracesConnector.(interface{ ForceCleanup(context.Context) }); ok {
		connectorImpl.ForceCleanup(ctx)
	}

	// Analyze all metrics
	allMetrics := mockConsumer.AllMetrics()
	errorCounts := make(map[string]int64) // key: "flowName:errorReason", value: count

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					metric := sm.Metrics().At(k)
					name := metric.Name()
					if strings.Contains(name, "_errors") {
						if metric.Type() == pmetric.MetricTypeSum {
							for dp := 0; dp < metric.Sum().DataPoints().Len(); dp++ {
								dataPoint := metric.Sum().DataPoints().At(dp)

								flowName := ""
								errorReason := ""

								if val, exists := dataPoint.Attributes().Get("flow_name"); exists {
									flowName = val.Str()
								}
								if val, exists := dataPoint.Attributes().Get("error_reason"); exists {
									errorReason = val.Str()
								}

								if flowName != "" && errorReason != "" {
									key := fmt.Sprintf("%s:%s", flowName, errorReason)
									// Keep the highest count seen (cumulative metrics)
									if dataPoint.IntValue() > errorCounts[key] {
										errorCounts[key] = dataPoint.IntValue()
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Verify expected counts
	assert.Equal(t, int64(2), errorCounts["flowA:failure"], "FlowA should have 2 failures")
	assert.Equal(t, int64(1), errorCounts["flowA:incomplete"], "FlowA should have 1 incomplete")
	assert.Equal(t, int64(1), errorCounts["flowB:failure"], "FlowB should have 1 failure")
	assert.Equal(t, int64(2), errorCounts["flowB:incomplete"], "FlowB should have 2 incompletes")
}
