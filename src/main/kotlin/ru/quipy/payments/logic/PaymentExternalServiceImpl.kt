package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = min(properties.rateLimitPerSec.toLong(),
        properties.parallelRequests * 1000 / properties.request95thPercentileProcessingTime.toMillis())
    private val parallelRequests = properties.parallelRequests
    private val cost = properties.cost

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newFixedThreadPool(parallelRequests)
    private var currentRequestsCount = AtomicInteger(0);

    private val connectionPool = ConnectionPool(parallelRequests, requestProcessingTime.toMillis(), TimeUnit.MILLISECONDS)
    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        connectTimeout(requestProcessingTime)
        readTimeout(requestProcessingTime)
        protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        connectionPool(connectionPool)
        build()
    }

    override fun getRateLimitPerSec(): Long {
        return rateLimitPerSec
    }

    override fun getCost(): Int {
        return cost
    }

    override fun isAvailable(): Boolean {
        return currentRequestsCount.get() < parallelRequests
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val timePassed = now() - paymentStartedAt;
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: $timePassed ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (timePassed + requestProcessingTime.toMillis() > paymentOperationTimeout.toMillis()) {
            logger.error("[$accountName] Payment rejected for txId: $transactionId, payment: $paymentId", "Timeout")

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Timeout")
            }
            return
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()
        currentRequestsCount.getAndIncrement()
        client.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onFailure(call: okhttp3.Call, e: IOException) {
                val requestsCount = currentRequestsCount.getAndDecrement()
                logger.error("[$accountName] [$requestsCount] Payment failed for txId: $transactionId, payment: $paymentId", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }

            override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                val requestsCount = currentRequestsCount.getAndDecrement()
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [$requestsCount/$parallelRequests] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[$accountName] [$requestsCount/$parallelRequests] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        })
    }
}

public fun now() = System.currentTimeMillis()