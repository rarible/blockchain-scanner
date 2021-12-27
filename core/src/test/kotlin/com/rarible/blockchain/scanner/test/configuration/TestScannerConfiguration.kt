package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.data.randomBlockchainData
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.data.testDescriptor2
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestLogService
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
    fun testLogRepository() = TestLogRepository(mongo)

    @Bean
    fun testLogService() = TestLogService(testLogRepository())

    @Bean
    fun testSubscriber1() = TestLogEventSubscriber(testDescriptor1(), 1)

    @Bean
    fun testSubscriber2() = TestLogEventSubscriber(testDescriptor2(), 2)

    @Bean
    fun testScanner(
        publisher: BlockEventPublisher,
        blockService: BlockService
    ): TestBlockchainScanner {
        val testLogRecordEventPublisher: LogRecordEventPublisher = mockk()

        // Imitates retry case for failed batch processing
        coEvery { testLogRecordEventPublisher.publish(any(), any()) }
            .returns(Unit)
            .andThenThrows(IllegalArgumentException("Test failure"))
            .andThen(Unit)

        return TestBlockchainScanner(
            blockchainClient = testBlockchainClient(),
            subscribers = listOf(testSubscriber1(), testSubscriber2()),
            blockService = blockService,
            logService = testLogService(),
            logRecordEventPublisher = testLogRecordEventPublisher,
            properties = properties
        )
    }
}
