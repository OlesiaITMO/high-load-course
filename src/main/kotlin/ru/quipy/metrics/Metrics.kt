package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component

@Component
class Metrics {
    val requestEventIncoming: Counter = Counter.builder("request_events")
        .description("Количество статусов выполняющихся запросов")
        .tag("status", "incoming")
        .register(Metrics.globalRegistry)

    val requestEventOutcoming: Counter = Counter.builder("request_events")
        .tag("status", "outcoming")
        .register(Metrics.globalRegistry)

    val requestEventCompleted: Counter = Counter.builder("request_events")
        .tag("status", "completed")
        .register(Metrics.globalRegistry)

    val ordersDuration: Timer = Timer.builder("orders_duration")
        .description("Время обработки одного заказа (полное)")
        .register(Metrics.globalRegistry)
}
