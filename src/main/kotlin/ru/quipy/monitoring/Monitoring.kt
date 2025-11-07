package ru.quipy.monitoring

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.TimeUnit

@Service
class MonitoringService {

    fun incRequestsCounter(type: String) {
        Counter.builder("httpRequests")
            .description("Number of requests by type")
            .tags("type", type)
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun incRetryCounter() {
        Counter.builder("httpRetry")
            .description("Number of attempts")
            .register(Metrics.globalRegistry)
            .increment()
    }

    fun recordRequestDuration(durationMs: Long, success: Boolean) {
        Timer.builder("RequestsDuration")
            .description("Duration with percentiles")
            .tags("success", success.toString())
            .publishPercentiles(0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    fun get90thPercentileTimeout(): Duration = Duration.ofMillis(DEFAULT_TIMEOUT_MS)

    companion object {
        private const val DEFAULT_TIMEOUT_MS = 1070L
    }
}


object RequestType {
    const val INCOMING = "INCOMING"
    const val OUTGOING = "OUTGOING"
    const val PROCESSED_SUCCESS = "PROCESSED_SUCCESS"
    const val PROCESSED_FAIL = "PROCESSED_FAIL"
}
