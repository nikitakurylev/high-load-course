package ru.quipy.payments.executor

import ru.quipy.payments.logic.ExternalServiceProperties

class PaymentExecutor(
        private val properties: List<ExternalServiceProperties>,
) {
    private val serviceLoad = properties.associateWith { 0.0 }.toMutableMap()
    private var lastLoadReset = System.currentTimeMillis()
    fun getOptimalProperties(): ExternalServiceProperties {
        val now = System.currentTimeMillis()
        if (now - lastLoadReset > 1000){
            lastLoadReset = now
            serviceLoad.replaceAll { _, _ -> 0.0 }
        }
        val optimalProperties = serviceLoad.minBy { (property, load) -> load / property.rateLimitPerSec }
        serviceLoad[optimalProperties.key] = optimalProperties.value + 1
        return optimalProperties.key
    }
}