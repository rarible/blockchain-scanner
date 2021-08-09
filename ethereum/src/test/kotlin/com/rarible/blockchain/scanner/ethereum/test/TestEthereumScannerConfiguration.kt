package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.ethereum.EnableEthereumBlockchainScanner
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumBlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventListener
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestBidSubscriber
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestTransferSubscriber
import io.mockk.mockk
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.web3j.utils.Numeric
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.transaction.MonoGasPriceProvider
import scalether.transaction.MonoSigningTransactionSender
import scalether.transaction.MonoSimpleNonceProvider
import scalether.transaction.MonoTransactionPoller
import java.math.BigInteger

@Configuration
@EnableEthereumBlockchainScanner
class TestEthereumScannerConfiguration {

    @Value("\${ethereumPrivateKey}")
    lateinit var privateKey: String

    @Autowired
    lateinit var properties: EthereumBlockchainScannerProperties

    @Bean
    fun testTransferSubscriber(): TestTransferSubscriber = TestTransferSubscriber()

    @Bean
    fun testBidSubscriber(): TestBidSubscriber = TestBidSubscriber()

    @Bean
    fun sender(ethereum: MonoEthereum) = MonoSigningTransactionSender(
        ethereum,
        MonoSimpleNonceProvider(ethereum),
        Numeric.toBigInt(privateKey),
        BigInteger.valueOf(8000000),
        MonoGasPriceProvider { Mono.just(BigInteger.ZERO) }
    )

    @Bean
    fun poller(ethereum: MonoEthereum) = MonoTransactionPoller(ethereum)

    @Bean
    fun testLogEventListener(): EthereumLogEventListener = mockk()

}