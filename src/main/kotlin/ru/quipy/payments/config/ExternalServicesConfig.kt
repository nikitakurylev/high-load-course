package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.executor.PaymentExecutor
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration


@Configuration
class ExternalServicesConfig {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        private val accountProps_1 = ExternalServiceProperties(
            // most expensive. Call costs 100
            "test",
            "default-1",
            parallelRequests = 10000,
            rateLimitPerSec = 100,
            request95thPercentileProcessingTime = Duration.ofMillis(1000),
            cost = 100
        )

        private val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 30,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            cost = 70
        )

        private val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            parallelRequests = 30,
            rateLimitPerSec = 8,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            cost = 40
        )

        // Call costs 30
        private val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            parallelRequests = 8,
            rateLimitPerSec = 5,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            cost = 30
        )
    }

    @Bean(PRIMARY_PAYMENT_BEAN)
    fun fastExternalService() =
        PaymentExecutor(listOf(
                paymentService4(),
                paymentService3(),
                paymentService2()
        ))

    @Bean("PAYMENT_BEAN_1")
    fun paymentService1() =
            PaymentExternalServiceImpl(
                    accountProps_1,
        )

    @Bean("PAYMENT_BEAN_2")
    fun paymentService2() =
            PaymentExternalServiceImpl(
                    accountProps_2,
            )

    @Bean("PAYMENT_BEAN_3")
    fun paymentService3() =
        PaymentExternalServiceImpl(
            accountProps_3,
        )

    @Bean("PAYMENT_BEAN_4")
    fun paymentService4() =
        PaymentExternalServiceImpl(
            accountProps_4,
        )
}