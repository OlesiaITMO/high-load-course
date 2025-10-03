package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component

@Component
class Metrics {
    val ordersCounter: Counter = Counter.builder("orders_counter")
        .description("Общее количество поступивших заказов")
        .tag("type", "order")
        .register(Metrics.globalRegistry)

    val ordersDuration: Timer = Timer.builder("orders_duration")
        .description("Время обработки одного заказа (полное)")
        .tag("type", "order")
        .publishPercentiles(0.500, 0.900, 0.999)
        .register(Metrics.globalRegistry)
}
