package com.rarible.blockchain.scanner.ethereum.test

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.ethereum.EnableEthereumScanner
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.repository.DefaultEthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestBidSubscriber
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestTransactionSubscriber
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestTransferSubscriber
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.spyk
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.web3jold.utils.Numeric
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.transaction.MonoSigningTransactionSender
import scalether.transaction.MonoSimpleNonceProvider
import scalether.transaction.MonoTransactionPoller
import java.math.BigInteger

@Configuration
@EnableEthereumScanner
@EnableAutoConfiguration
@EnableMongock
class TestEthereumScannerConfiguration {

    @Value("\${ethereumPrivateKey}")
    lateinit var privateKey: String

    @Autowired
    lateinit var properties: EthereumScannerProperties

    @Bean
    fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()

    @Bean
    fun testTransferRepository(mongo: ReactiveMongoOperations): DefaultEthereumLogRepository {
        return DefaultEthereumLogRepository(mongo, "transfer")
    }

    @Bean
    fun testTransferSubscriber(testTransferRepository: DefaultEthereumLogRepository): TestTransferSubscriber {
        return TestTransferSubscriber(testTransferRepository)
    }

    @Bean
    fun testBidRepository(mongo: ReactiveMongoOperations): DefaultEthereumLogRepository {
        return DefaultEthereumLogRepository(mongo, "bid")
    }

    @Bean
    fun testBidSubscriber(testBidRepository: DefaultEthereumLogRepository): TestBidSubscriber {
        return TestBidSubscriber(testBidRepository)
    }

    @Bean
    fun testTransactionSubscriber(): TestTransactionSubscriber = TestTransactionSubscriber()

    @Bean
    fun sender(ethereum: MonoEthereum) = MonoSigningTransactionSender(
        ethereum,
        MonoSimpleNonceProvider(ethereum),
        Numeric.toBigInt(privateKey),
        BigInteger.valueOf(8000000)
    ) { Mono.just(BigInteger.ZERO) }

    @Bean
    fun poller(ethereum: MonoEthereum) = MonoTransactionPoller(ethereum)

    @Bean
    @Primary
    @Qualifier("testEthereumBlockchainClient")
    fun testEthereumBlockchainClient(originalClient: EthereumBlockchainClient): EthereumBlockchainClient =
        spyk(TestEthereumBlockchainClient(originalClient))

    @Bean
    @Primary
    @Qualifier("TestEthereumLogEventPublisher")
    fun testEthereumLogEventPublisher(): LogRecordEventPublisher = TestEthereumLogRecordEventPublisher()

    @Bean
    @Primary
    @Qualifier("TestEthereumTransactionEventPublisher")
    fun testEthereumTransactionEventPublisher(): TransactionRecordEventPublisher =
        TestEthereumTransactionRecordEventPublisher()
}
