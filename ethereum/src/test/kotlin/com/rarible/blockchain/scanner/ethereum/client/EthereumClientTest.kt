package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.ethereum.configuration.BlockPollerProperties
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.reduce.createRandomEthereumLog
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomInt
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.domain.response.Log
import java.math.BigInteger

@Suppress("ReactiveStreamsUnusedPublisher")
@ExtendWith(MockKExtension::class)
class EthereumClientTest {
    private val properties = mockk<EthereumScannerProperties> {
        every { blockchain } returns "ethereum"
        every { monitoring } returns MonitoringProperties()
        every { retryPolicy } returns RetryPolicyProperties()
        every { maxBatches } returns emptyList()
        every { blockPoller } returns BlockPollerProperties()
        every { scan } returns ScanProperties()
    }
    private val ethPubSub = mockk<EthPubSub> {
        every { newHeads() } returns Flux.empty()
    }

    @MockK
    private lateinit var ethereum: MonoEthereum

    @MockK
    private lateinit var monitor: BlockchainMonitor

    @InjectMockKs
    private lateinit var ethereumClient: EthereumClient

    @Test
    fun `get first available block as current`() = runBlocking<Unit> {
        val currentBlock = BigInteger.valueOf(randomInt().toLong())
        every { ethereum.ethBlockNumber() } returns Mono.just(currentBlock)
        every { ethereum.ethGetFullBlockByNumber(currentBlock) } returns Mono.just(ethBlock(currentBlock.intValueExact()))
        every { properties.scan } returns ScanProperties(startFromCurrentBlock = true)

        val block = ethereumClient.getFirstAvailableBlock()
        assertThat(block.number).isEqualTo(currentBlock.toLong())

        verify {
            ethereum.ethGetFullBlockByNumber(currentBlock)
        }
    }

    @Test
    fun `skip logs from epoch block`() = runBlocking<Unit> {
        val epochBlock = BigInteger.valueOf(randomInt().toLong())
        val ethBlock = ethBlock(epochBlock.intValueExact())

        every { properties.ignoreEpochBlocks } returns true
        every { properties.ignoreNullableLogs } returns false
        every { ethereum.ethBlockNumber() } returns Mono.just(epochBlock)
        every { ethereum.ethGetFullBlockByNumber(epochBlock) } returns Mono.just(ethBlock)
        every { properties.scan } returns ScanProperties(startFromCurrentBlock = true)

        val log = mockk<Log> {
            every { removed() } returns false
            every { transactionHash() } returns ethBlock.hash()
            every { blockHash() } returns ethBlock.hash()
        }
        every { ethereum.ethGetLogsJava(any()) } returns Mono.just(listOf(log))

        val descriptor = mockk<EthereumDescriptor> {
            every { contracts } returns listOf(randomAddress())
            every { ethTopic } returns randomWord()
        }
        val ethereumBlockchainBlock = EthereumBlockchainBlock(ethBlock)

        val logs = ethereumClient.getBlockLogs(
            descriptor = descriptor,
            blocks = listOf(ethereumBlockchainBlock),
            stable = true
        ).toList()

        assertThat(logs.single().logs).isEmpty()
    }
}
