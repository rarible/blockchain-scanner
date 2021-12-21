package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.reconciliation.ReconciliationFromProvider
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.data.randomBlockchainData
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.data.testDescriptor2
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestBlockService
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.DefaultTestLogRecordComparator
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import io.mockk.coEvery
import io.mockk.mockk
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.ReactiveMongoOperations

@Configuration
@EnableAutoConfiguration
@EnableRaribleMongo
@EnableBlockchainScanner
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

    @Bean
    fun testBlockchainClient() = TestBlockchainClient(
        randomBlockchainData(
            TEST_BLOCK_COUNT, TEST_LOG_COUNT_PER_BLOCK,
            testDescriptor1().id, testDescriptor2().id
        )
    )

    @Bean
    fun testBlockMapper() = TestBlockMapper()

    @Bean
    fun testBlockRepository() = TestBlockRepository(mongo)

    @Bean
    fun testLogRepository() = TestLogRepository(mongo)

    @Bean
    fun testBlockService() = TestBlockService(testBlockRepository())

    @Bean
    fun testLogService() = TestLogService(testLogRepository())

    @Bean
    fun testSubscriber1() = TestLogEventSubscriber(testDescriptor1(), 1)

    @Bean
    fun testSubscriber2() = TestLogEventSubscriber(testDescriptor2(), 2)

    @Bean
    fun testLogRecordComparator() = DefaultTestLogRecordComparator()

    @Bean
    fun testScanner(consumer: BlockEventConsumer, publisher: BlockEventPublisher): TestBlockchainScanner {
        val testLogEventPublisher: LogEventPublisher = mockk()

        // Imitates retry case for failed batch processing
        coEvery { testLogEventPublisher.publish(any(), any(), any()) }
            .returns(Unit)
            .andThenThrows(IllegalArgumentException("Test failure"))
            .andThen(Unit)

        return TestBlockchainScanner(
            blockchainClient = testBlockchainClient(),
            subscribers = listOf(testSubscriber1(), testSubscriber2()),
            blockMapper = testBlockMapper(),
            blockService = testBlockService(),
            logService = testLogService(),
            logEventPublisher = testLogEventPublisher,
            logEventComparator = testLogRecordComparator(),
            properties = properties,
            blockEventConsumer = consumer,
            blockEventPublisher = publisher
        )
    }

    @Bean
    fun fromProvider(): ReconciliationFromProvider = object : ReconciliationFromProvider {
        override fun initialFrom(groupId: String): Long = 1L
    }

}
