package ru.quipy.payments.executor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.time.Duration
import java.util.*

const val ONE_SECOND_IN_MILLIS: Long = 1000

class PaymentExecutor(paymentServices: List<PaymentExternalService>) : PaymentService {

    val logger: Logger = LoggerFactory.getLogger(PaymentExecutor::class.java)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
    private val serviceLoads = paymentServices.associateWith { 0 }.toMutableMap()
    private var lastLoadReset = System.currentTimeMillis()
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        synchronized(serviceLoads) {
            var optimalService: Map.Entry<PaymentExternalService, Int>?
            do {
                val now = System.currentTimeMillis()
                if (now - lastLoadReset > ONE_SECOND_IN_MILLIS) {
                    lastLoadReset = now
                    serviceLoads.replaceAll { _, _ -> 0 }
                }

                optimalService = serviceLoads
                    .filter { (service, load) -> load <= service.getRateLimitPerSec() && service.isAvailable() }
                    .minByOrNull { (service, _) -> service.getCost() }
            } while (optimalService == null)

            serviceLoads[optimalService.key] = optimalService.value + 1
            logger.warn(serviceLoads.values.joinToString())

            optimalService.key.submitPaymentRequest(paymentId, amount, paymentStartedAt)
        }
    }

    override fun submitError(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        PaymentExternalServiceImpl.logger.error("[no account] Payment failed for txId: $transactionId, payment: $paymentId")

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Timeout")
        }
    }
}