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
