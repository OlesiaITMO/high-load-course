package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.ktor.client.*
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.client.request.*
import io.ktor.client.statement.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.Metrics
import ru.quipy.common.utils.NonBlockingSlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.time.Duration as KtDuration
import kotlin.time.Duration.Companion.milliseconds

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val dbScope: CoroutineScope,
    private val metrics: Metrics
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        const val REQUEST_TIMEOUT = 1500L
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val semaphore = Semaphore(permits = parallelRequests)

    private val client = HttpClient(Java) {
        engine {
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }

        expectSuccess = false

        install(io.ktor.client.plugins.HttpTimeout) {
            requestTimeoutMillis = REQUEST_TIMEOUT
        }
    }

    private val circuitBreaker = CircuitBreaker.of(
        "paymentService-$accountName",
        CircuitBreakerConfig.custom()
            .failureRateThreshold(5F)
            .slowCallRateThreshold(5F)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .slowCallDurationThreshold(Duration.ofSeconds(1))
            .permittedNumberOfCallsInHalfOpenState(40)
            .build()
    )

    private val rateLimiter: NonBlockingSlidingWindowRateLimiter by lazy {
        NonBlockingSlidingWindowRateLimiter(rate = rateLimitPerSec)
    }

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        val startedAt = now()
        dbScope.launch {
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            while (true) {
                try {
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            startedAt,
                            Duration.ofMillis(startedAt - paymentStartedAt)
                        )
                    }
                    break
                } catch (_: IllegalArgumentException) {
                    delay(10)
                }
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val breakerStartNanos = System.nanoTime()

        val result = try {
            if (!circuitBreaker.tryAcquirePermission()) {
                logger.warn(
                    "[$accountName] Circuit breaker is OPEN. Payment request skipped for paymentId=$paymentId, txId=$transactionId"
                )
                Result(false, "Circuit breaker open")
            } else {
                val hedgedResult = hedged(delay = 200.milliseconds, maxAttempts = 5) {
                    send(paymentId, amount, transactionId, paymentStartedAt)
                }

                val durationNanos = System.nanoTime() - breakerStartNanos

                if (hedgedResult.status) {
                    circuitBreaker.onSuccess(durationNanos, TimeUnit.NANOSECONDS)
                } else {
                    circuitBreaker.onError(
                        durationNanos,
                        TimeUnit.NANOSECONDS,
                        PaymentProviderException(hedgedResult.message ?: "Payment provider returned unsuccessful result")
                    )
                }

                hedgedResult
            }
        } catch (e: Exception) {
            val durationNanos = System.nanoTime() - breakerStartNanos
            circuitBreaker.onError(durationNanos, TimeUnit.NANOSECONDS, e)

            logger.error(
                "[$accountName] Payment execution failed under circuit breaker for txId: $transactionId, payment: $paymentId.",
                e
            )
            Result(false, e.message)
        }

        val processedAt = now()
        dbScope.launch {
            while (true) {
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(result.status, processedAt, transactionId, reason = result.message)
                    }
                    break
                } catch (_: IllegalArgumentException) {
                    delay(10)
                }
            }
        }
    }

    suspend fun send(paymentId: UUID, amount: Int, transactionId: UUID, paymentStartedAt: Long): Result {
        try {
            while (!circuitBreaker.tryAcquirePermission()) {
                delay(10)
            }

            semaphore.acquire()

            if (!rateLimiter.acquireSuspend(200L)) {
                return Result(false, "Rate limit exceeded")
            }

            val response =
                client.post("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")

            val responseText = response.bodyAsText()

            val body = try {
                mapper.readValue(responseText, ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error(
                    "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.status.value}, reason: $responseText",
                    e
                )
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.info(
                "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}."
            )
            return Result(body.result, body.message)
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId.", e)
                }

                is CancellationException -> {}
                is HttpRequestTimeoutException -> {}

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId.", e)
                }
            }
            return Result(false, e.message)
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    data class Result(val status: Boolean, val message: String?)

    class PaymentProviderException(message: String) : RuntimeException(message)

    suspend fun <T> hedged(
        delay: KtDuration,
        maxAttempts: Int,
        block: suspend () -> T
    ): T = coroutineScope {
        val deferreds = mutableListOf<Deferred<T>>()
        val result = CompletableDeferred<T>()

        try {
            repeat(maxAttempts) { i ->
                deferreds += async {
                    val value = block()
                    result.complete(value)
                    value
                }
                if (i < maxAttempts - 1) {
                    withTimeoutOrNull(delay) { result.await() }
                        ?.let {
                            metrics.hedgedAttempts.record((i + 1).toDouble())
                            return@coroutineScope it
                        }
                }
            }

            val value = result.await()
            metrics.hedgedAttempts.record(maxAttempts.toDouble())
            value
        } finally {
            deferreds.forEach { it.cancel() }
        }
    }
}

fun now() = System.currentTimeMillis()