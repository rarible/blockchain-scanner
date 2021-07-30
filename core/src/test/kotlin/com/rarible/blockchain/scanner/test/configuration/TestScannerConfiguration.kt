package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.test.TestScanner
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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.ReactiveMongoOperations

@Configuration
@EnableRaribleMongo
@EnableConfigurationProperties(TestBlockchainScannerProperties::class)
class TestScannerConfiguration {

    @Autowired
    lateinit var properties: TestBlockchainScannerProperties

    @Autowired
    lateinit var mongo: ReactiveMongoOperations

    @Bean
    fun testBlockchainClient() = TestBlockchainClient(randomBlockchainData(10, 5, testDescriptor1().topic))
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
    fun testSubscriber2() = TestLogEventSubscriber(testDescriptor2(), 1)

    @Bean
    fun testScanner() = TestScanner(
        testBlockchainClient(),
        listOf(testSubscriber1(), testSubscriber2()),
        testBlockMapper(),
        testBlockService(),
        testLogMapper(),
        testLogService(),
        listOf(),
        testPendingLogService(),
        listOf(),
        properties
    )

}