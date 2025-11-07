package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import org.springframework.stereotype.Service


@Service
class MetricsService {
    private var retryCounter = Counter.builder("request_retries")
        .description("Count of retries")
        .register(Metrics.globalRegistry)

    private var requestLatency = DistributionSummary.builder("request_latency")
        .description("Request latency")
        .publishPercentiles(0.5, 0.85, 0.89, 0.9, 0.95, 0.99)
        .register(Metrics.globalRegistry)

    fun incRetryCounter() {
        retryCounter.increment()
    }

    fun addRequestDuration(durationMs: Long) {
        requestLatency.record(durationMs.toDouble())
    }
}

