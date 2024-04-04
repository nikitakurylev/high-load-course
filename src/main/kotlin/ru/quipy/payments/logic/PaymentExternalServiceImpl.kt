package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import javax.annotation.PostConstruct
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
) : PaymentExternalService {

    @PostConstruct
    fun init() {
        startListening()
    }

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestProcessingTime = properties.request95thPercentileProcessingTime
    private val rate = min(
        properties.rateLimitPerSec.toFloat(),
        (properties.parallelRequests.toFloat() / properties.request95thPercentileProcessingTime.seconds.toFloat())
    )
    private val parallelRequests = properties.parallelRequests
    private val cost = properties.cost
    private val window = OngoingWindow(parallelRequests)
    private val rateLimiter = RateLimiter(properties.rateLimitPerSec)
    private val paymentsQueue: BlockingQueue<Runnable> = LinkedBlockingQueue()

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val paymentExecutor = ThreadPoolExecutor(100, 100, paymentOperationTimeout.seconds, TimeUnit.SECONDS, ArrayBlockingQueue(5000), NamedThreadFactory("payment"), CallerRunsPolicy());//Executors.newFixedThreadPool(100, NamedThreadFactory("payment"))
    private val responseExecutor = ThreadPoolExecutor(100, 100, paymentOperationTimeout.seconds, TimeUnit.SECONDS, ArrayBlockingQueue(5000), NamedThreadFactory("response"), CallerRunsPolicy())

    private val requestExecutor = ThreadPoolExecutor(100, 100, paymentOperationTimeout.seconds, TimeUnit.SECONDS, ArrayBlockingQueue(5000), NamedThreadFactory("request"), CallerRunsPolicy())

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(requestExecutor).apply { maxRequests = parallelRequests; maxRequestsPerHost = parallelRequests })
        protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }

    override fun getCost(): Int {
        return cost
    }

    override fun getQueueTime(): Float {
        return ((paymentsQueue.size + 1) / rate)
    }

    override fun enqueuePaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        paymentsQueue.put { submitPaymentRequest(paymentId, amount, paymentStartedAt) }
    }

    private fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (now() - paymentStartedAt + requestProcessingTime.toMillis() > paymentOperationTimeout.toMillis()) {
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


        client.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onFailure(call: okhttp3.Call, e: IOException) {
                responseExecutor.submit {
                    window.release()
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }

            override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                responseExecutor.submit {
                    window.release()
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
            }
        })
    }

    private fun startListening() {
        paymentExecutor.submit {
            while (true) {
                window.acquire()
                rateLimiter.tickBlocking()

                val task = paymentsQueue.take()
                paymentExecutor.execute(task)
            }
        }
    }
}

public fun now() = System.currentTimeMillis()