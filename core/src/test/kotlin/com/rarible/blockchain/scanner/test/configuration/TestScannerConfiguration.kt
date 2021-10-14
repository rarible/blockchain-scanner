package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.reconciliation.ReconciliationFromProvider
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.data.randomBlockchainData
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.data.testDescriptor2
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.mapper.TestLogMapper
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestBlockService
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.service.TestPendingLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.ReactiveMongoOperations

@FlowPreview
@ExperimentalCoroutinesApi
@Configuration
@EnableAutoConfiguration
@EnableRaribleMongo
@EnableConfigurationProperties(TestBlockchainScannerProperties::class)
class TestScannerConfiguration {

    companion object {
        const val TEST_BLOCK_COUNT = 10
        const val TEST_LOG_COUNT_PER_BLOCK = 5
    }

    @Autowired
    lateinit var properties: TestBlockchainScannerProperties

    @Autowired
    lateinit var mongo: ReactiveMongoOperations

    @Autowired
    lateinit var metrics: com.rarible.blockchain.scanner.metrics.Metrics

    @Bean
    fun testBlockchainClient() = TestBlockchainClient(
        randomBlockchainData(
            TEST_BLOCK_COUNT, TEST_LOG_COUNT_PER_BLOCK,
            testDescriptor1().topic, testDescriptor2().topic
        )
    )

    @Bean
    fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()

    @Bean
    fun testBlockMapper() = TestBlockMapper()

    @Bean
    fun testLogMapper() = TestLogMapper()

    @Bean
    fun testBlockRepository() = TestBlockRepository(mongo)

    @Bean
    fun testLogRepository() = TestLogRepository(mongo)

    @Bean
    fun testBlockService() = TestBlockService(testBlockRepository())

    @Bean
    fun testLogService() = TestLogService(testLogRepository())

    @Bean
    fun testPendingLogService() = TestPendingLogService()

    @Bean
    fun testSubscriber1() = TestLogEventSubscriber(testDescriptor1(), 1)

    @Bean
    fun testSubscriber2() = TestLogEventSubscriber(testDescriptor2(), 2)

    @Bean
    fun testScanner() = TestBlockchainScanner(
        metrics,
        blockchainClient = testBlockchainClient(),
        subscribers = listOf(testSubscriber1(), testSubscriber2()),
        blockMapper = testBlockMapper(),
        blockService = testBlockService(),
        logMapper = testLogMapper(),
        logService = testLogService(),
        pendingLogService = testPendingLogService(),
        logEventListeners = listOf(),
        properties = properties
    )

    @Bean
    fun fromProvider(): ReconciliationFromProvider = object : ReconciliationFromProvider {
        override fun initialFrom(descriptorId: String): Long = 1L
    }

}
