package ru.quipy.payments.executor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.payments.logic.PaymentExternalService
import ru.quipy.payments.logic.PaymentService
import java.util.*

const val ONE_SECOND_IN_MILLIS: Long = 1000

class PaymentExecutor(paymentServices: List<PaymentExternalService>) : PaymentService {

    val logger: Logger = LoggerFactory.getLogger(PaymentExecutor::class.java)
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
}