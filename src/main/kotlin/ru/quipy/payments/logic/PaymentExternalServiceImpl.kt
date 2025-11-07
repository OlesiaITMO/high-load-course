package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.math.pow


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService
) : PaymentExternalSystemAdapter {

    private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    private val emptyBody = RequestBody.create(null, ByteArray(0))
    private val mapper = ObjectMapper().registerKotlinModule()

    object PaymentRetryConfig {
        const val BASE = 2.0
        const val COEFF = 0.225
        const val MAX_RETRIES = 5
    }


    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client: OkHttpClient by lazy {
        val timeout = averageProcessingTime()
        OkHttpClient.Builder()
            .callTimeout(timeout)
            .connectTimeout(timeout)
            .readTimeout(timeout)
            .writeTimeout(timeout)
            .build()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val deadlinems = deadline * 1000

        if (now() > deadlinems) {
            monitoringService.incRequestsCounter(RequestType.PROCESSED_FAIL)
            return
        }

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")


        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()
            for (i in 1..PaymentRetryConfig.MAX_RETRIES) {
                try {
                    if (sendRequest(request, paymentId, transactionId)) {
                        break
                    }
                } catch (e: InterruptedIOException) {
                    if (i == PaymentRetryConfig.MAX_RETRIES) {
                        logger.error("[$accountName] Payment timeout after all retries for txId: $transactionId, payment: $paymentId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout after ${PaymentRetryConfig.MAX_RETRIES} retries.")
                        }
                        monitoringService.incRequestsCounter(RequestType.PROCESSED_FAIL)
                        return
                    }
                }

                if (i > 1) {
                    monitoringService.incRetryCounter()
                }

                val delay = (PaymentRetryConfig.COEFF * PaymentRetryConfig.BASE.pow(i)).toLong()

                if (deadlinems < System.currentTimeMillis() + delay) {
                    monitoringService.incRequestsCounter(RequestType.PROCESSED_FAIL)
                    return
                }

                if (i < PaymentRetryConfig.MAX_RETRIES) {
                    Thread.sleep(delay)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun sendRequest(request: Request, paymentId: UUID, transactionId: UUID): Boolean {
        val startTime = System.currentTimeMillis()

        return try {
            client.newCall(request).execute().use { response ->
                monitoringService.incRequestsCounter(RequestType.OUTGOING)

                val body = runCatching {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                }.getOrElse { e ->
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, response code: ${response.code}, error: ${e.message}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                val requestType = if (body.result) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                monitoringService.incRequestsCounter(requestType)

                val duration = System.currentTimeMillis() - startTime
                monitoringService.recordRequestDuration(duration, body.result)

                // Обновление состояния оплаты в базе данных
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                body.result
            }
        } catch (e: Exception) {
            val duration = System.currentTimeMillis() - startTime
            logger.error("[$accountName] Payment request failed for txId: $transactionId, payment: $paymentId, duration: $duration ms", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            false
        }
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    fun rateLimitPerSec() = properties.rateLimitPerSec

    override fun name() = properties.accountName

    fun averageProcessingTime() = properties.averageProcessingTime

}

public fun now() = System.currentTimeMillis()