package ru.quipy.payments.executor

import kotlinx.coroutines.sync.Mutex
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.payments.logic.PaymentExternalService
import ru.quipy.payments.logic.PaymentService
import ru.quipy.payments.subscribers.OrderPaymentSubscriber
import java.util.*

const val ONE_SECOND_IN_MILLIS = 1000

class PaymentExecutor(paymentServices: List<PaymentExternalService>) : PaymentService {

    val logger: Logger = LoggerFactory.getLogger(PaymentExecutor::class.java)
    private val serviceLoads = paymentServices.associateWith { 0 }.toMutableMap()
    private var lastLoadReset = System.currentTimeMillis()
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        synchronized(serviceLoads) {
            var optimalService = serviceLoads
                    .filter { (property, _) -> property.getCurrentRequestsCount() < property.getProperties().parallelRequests }
                    .minByOrNull { (property, load) -> load.toFloat() / property.getProperties().rateLimitPerSec.toFloat() }
            if (optimalService == null)
                optimalService = serviceLoads.minBy { (property, _) -> property.getProperties().request95thPercentileProcessingTime }

            val now = System.currentTimeMillis()
            if (now - lastLoadReset > ONE_SECOND_IN_MILLIS) {
                lastLoadReset = now
                serviceLoads.replaceAll { _, _ -> 0 }
            }
            serviceLoads[optimalService.key] = optimalService.value + 1
            logger.warn(serviceLoads.values.joinToString())

            optimalService.key.submitPaymentRequest(paymentId, amount, paymentStartedAt)
        }
    }

}