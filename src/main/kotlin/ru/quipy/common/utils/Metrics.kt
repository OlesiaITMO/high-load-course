package ru.quipy.common.utils

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component

@Component
class Metrics {
    val hedgedAttempts: DistributionSummary = DistributionSummary.builder("hedged_request_events")
        .description("Количество запросов на один платеж")
        .register(Metrics.globalRegistry)
}