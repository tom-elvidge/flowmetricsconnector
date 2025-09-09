# flowmetricsconnector

This is an OpenTelemetry connector that listens for span events, correlates a defined start event with a corresponding end event, and exposes RED (Rate, Errors, Duration) metrics for each resulting flow.

Unlike the official spanmetricsconnector, which treats entire spans as the unit for metrics, this connector defines a flow between specific span events rather than the full span, allowing fine-grained monitoring of custom start-to-end activities across spans.

## Example

```text
flow.order_processing.start
  Attributes:
    correlation-id: order-123

flow.order_processing.end
  Attributes:
    correlation-id: order-123
    outcome: success
```

End events have an outcome of either `success`, `failure` or `cancelled`. Success flows record latency metrics, failed and cancelled flows do not.

Flows which are started but never ended are cleaned up after a timeout, they are recorded as errors with the dimension `error_reason=incomplete` and no latency metric.

## Development

1. Install the OTel Collector Builder
    https://opentelemetry.io/docs/collector/custom-collector/#step-1---install-the-builder

2. Build a test collector distrbution bundled with flowmetrics from source.
    ```sh
    ./ocb --config builder-config.yaml
    ```

3. Run the test collector.
    ```sh
    ./otelcol-dev/otelcol-dev --config collector-config.yaml
    ```

4. Send spans with a flow start event.
    ```sh
    curl -X POST http://localhost:4318/v1/traces \
        -H "Content-Type: application/json" \
        -d '{
        "resourceSpans": [{
            "resource": {
            "attributes": [{
                "key": "service.name",
                "value": { "stringValue": "order-service" }
            }]
            },
            "scopeSpans": [{
            "spans": [{
                "traceId": "99999999999999999999999999999999",
                "spanId": "1111111111111111",
                "name": "create-order",
                "startTimeUnixNano": "'$(date +%s)000000000'",
                "endTimeUnixNano": "'$(date +%s)000000000'",
                "events": [{
                "timeUnixNano": "'$(date +%s)000000000'",
                "name": "flow.order_processing.start",
                "attributes": [{
                    "key": "correlation-id",
                    "value": { "stringValue": "order-001" }
                }]
                }]
            }]
            }]
        }]
        }'
    ```

5. Send spans with a flow end event.
    ```sh
    curl -X POST http://localhost:4318/v1/traces \
        -H "Content-Type: application/json" \
        -d '{
        "resourceSpans": [{
            "resource": {
            "attributes": [{
                "key": "service.name",
                "value": { "stringValue": "fulfillment-service" }
            }]
            },
            "scopeSpans": [{
            "spans": [{
                "traceId": "88888888888888888888888888888888",
                "spanId": "2222222222222222",
                "name": "fulfill-order",
                "startTimeUnixNano": "'$(date +%s)000000000'",
                "endTimeUnixNano": "'$(date +%s)000000000'",
                "events": [{
                "timeUnixNano": "'$(date +%s)000000000'",
                "name": "flow.order_processing.end",
                "attributes": [{
                    "key": "correlation-id",
                    "value": { "stringValue": "order-001" }
                }, {
                    "key": "outcome",
                    "value": { "stringValue": "success" }
                }]
                }]
            }]
            }]
        }]
        }'
    ```

6. Optionally send spans with a flow end event as `cancelled` or `failed`.
    ```sh
    curl -X POST http://localhost:4318/v1/traces \
        -H "Content-Type: application/json" \
        -d '{
        "resourceSpans": [{
            "resource": {
            "attributes": [{
                "key": "service.name",
                "value": { "stringValue": "fulfillment-service" }
            }]
            },
            "scopeSpans": [{
            "spans": [{
                "traceId": "88888888888888888888888888888888",
                "spanId": "2222222222222222",
                "name": "fulfill-order",
                "startTimeUnixNano": "'$(date +%s)000000000'",
                "endTimeUnixNano": "'$(date +%s)000000000'",
                "events": [{
                "timeUnixNano": "'$(date +%s)000000000'",
                "name": "flow.order_processing.end",
                "attributes": [{
                    "key": "correlation-id",
                    "value": { "stringValue": "order-001" }
                }, {
                    "key": "outcome",
                    "value": { "stringValue": "cancelled" }
                }]
                }]
            }]
            }]
        }]
        }'
    ```