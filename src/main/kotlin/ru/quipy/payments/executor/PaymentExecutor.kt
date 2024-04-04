package ru.quipy.payments.executor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import ru.quipy.payments.logic.PaymentExternalServiceImpl.Companion.paymentOperationTimeout
import java.time.Duration
import java.util.*

class PaymentExecutor(private val paymentServices: List<PaymentExternalService>) : PaymentService {

    val logger: Logger = LoggerFactory.getLogger(PaymentExecutor::class.java)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val optimalService = paymentServices
            .filter { service -> now() - paymentStartedAt + service.getQueueTime() * 1000 < paymentOperationTimeout.toMillis() }
            .minByOrNull { service -> service.getCost() }

        if (optimalService == null) {
            submitError(paymentId, amount, paymentStartedAt)
            return
        }

        optimalService.enqueuePaymentRequest(paymentId, amount, paymentStartedAt)
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