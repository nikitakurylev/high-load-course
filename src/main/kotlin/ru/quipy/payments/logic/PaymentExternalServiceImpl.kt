package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.executor.PaymentExecutor
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val executor: PaymentExecutor,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private var httpClientExecutors: MutableMap<ExternalServiceProperties, ExecutorService> = mutableMapOf()

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val optimalProperties = executor.getOptimalProperties()
        val serviceName = optimalProperties.serviceName
        val accountName = optimalProperties.accountName
        val requestAverageProcessingTime = optimalProperties.request95thPercentileProcessingTime
        val rateLimitPerSec = optimalProperties.rateLimitPerSec
        val parallelRequests = optimalProperties.parallelRequests

        if(!httpClientExecutors.containsKey(optimalProperties))
            httpClientExecutors[optimalProperties] = Executors.newFixedThreadPool(parallelRequests)
        val httpClientExecutor = httpClientExecutors[optimalProperties]!!

        val client = OkHttpClient.Builder().run {
            connectTimeout(paymentOperationTimeout.toMillis(), TimeUnit.MILLISECONDS)
            dispatcher(Dispatcher(httpClientExecutor))
            build()
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onFailure(call: okhttp3.Call, e: IOException) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }

            override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

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