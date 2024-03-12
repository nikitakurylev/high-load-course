package ru.quipy.payments.logic

import java.time.Duration
import java.util.*

interface PaymentService {
    /**
     * Submit payment request to external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)
    fun submitError(paymentId: UUID, amount: Int, paymentStartedAt: Long)
}

interface PaymentExternalService {
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)
    fun getRateLimitPerSec() : Long
    fun getCost() : Int
    fun isAvailable() : Boolean
}

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11),
    val cost: Int
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)