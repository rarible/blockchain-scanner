package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.ethLog
import com.rarible.blockchain.scanner.ethereum.test.data.ethTransaction
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import com.rarible.core.test.data.randomAddress
import io.daonomic.rpc.domain.Word
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toMono
import scala.jdk.javaapi.CollectionConverters
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.domain.response.Block
import scalether.domain.response.Transaction

class EthereumBlockchainLogIndexTest {
    private val monitor = BlockchainMonitor(SimpleMeterRegistry())

    @Test
    fun `index is calculated in group of transactionHash, topic, address`() = runBlocking {
        val blockHash0 = randomWord()
        val blockHash1 = randomWord()
        val blockHash2 = randomWord()
        val blockHash3 = randomWord()
        val blockHash4 = randomWord()
        val blockHash5 = randomWord()

        val transactionHash1 = randomWord()
        val topic1 = randomWord()
        val address1 = randomAddress()

        val transactionHash2 = randomWord()
        val topic2 = randomWord()
        val address2 = randomAddress()

        fun createEthereumBL(
            index: Int,
            transactionHash: Word,
            topic: Word,
            address: Address,
            logIndex: Int,
            blockHash: Word,
            blockNumber: Long
        ) = EthereumBlockchainLog(
            ethLog = ethLog(
                transactionHash = transactionHash,
                topic = topic,
                address = address,
                logIndex = logIndex,
                blockHash = blockHash
            ),
            ethTransaction = ethTransaction(
                transactionHash = transactionHash,
                blockHash = blockHash,
                blockNumber = blockNumber.toBigInteger()
            ),
            index = index,
            total =  1
        )

        val expectedLogs = listOf(
            /* 0 */
            createEthereumBL(0, randomWord(), topic1, randomAddress(), 0, blockHash0, 0),
            /* 1 */
            createEthereumBL(0, randomWord(), randomWord(), address1, 1, blockHash1, 1),
            /* 2 */
            createEthereumBL(0, randomWord(), topic1, address1, 2, blockHash2, 2),
            /* 3 */
            createEthereumBL(0, randomWord(), randomWord(), address2, 3, blockHash3, 3),

            // Group #1 of <transactionHash, topic, address>
            /* 4 */
            createEthereumBL(0, transactionHash1, topic1, address1, 4, blockHash4, 4),
            /* 5 */
            createEthereumBL(1, transactionHash1, topic1, address1, 5, blockHash4, 4),
            /* 6 */
            createEthereumBL(2, transactionHash1, topic1, address1, 6, blockHash4, 4),

            // Group #2 of <transactionHash, topic, address>
            /* 7 */
            createEthereumBL(0, transactionHash2, topic2, address2, 7, blockHash5, 5),
            /* 8 */
            createEthereumBL(1, transactionHash2, topic2, address2, 8, blockHash5, 5),
            /* 9 */
            createEthereumBL(2, transactionHash2, topic2, address2, 9, blockHash5, 5),
        )
        val allBlocks = listOf(
            ethBlock(0, blockHash0, expectedLogs),
            ethBlock(1, blockHash1, expectedLogs),
            ethBlock(2, blockHash2, expectedLogs),
            ethBlock(3, blockHash3, expectedLogs),
            ethBlock(4, blockHash4, expectedLogs),
            ethBlock(5, blockHash5, expectedLogs)
        )

        val expectedFullBlocks = listOf(
            FullBlock(
                block = EthereumBlockchainBlock(allBlocks[0]),
                logs = listOf(expectedLogs[0])
            ),
            FullBlock(
                block = EthereumBlockchainBlock(allBlocks[1]),
                logs = listOf(expectedLogs[1])
            ),
            FullBlock(
                block = EthereumBlockchainBlock(allBlocks[2]),
                logs = listOf(expectedLogs[2])
            ),
            FullBlock(
                block = EthereumBlockchainBlock(allBlocks[3]),
                logs = listOf(expectedLogs[3])
            ),
            FullBlock(
                block = EthereumBlockchainBlock(allBlocks[4]),
                logs = listOf(expectedLogs[4], expectedLogs[5], expectedLogs[6])
            ),
            FullBlock(
                block = EthereumBlockchainBlock(allBlocks[5]),
                logs = listOf(expectedLogs[7], expectedLogs[8], expectedLogs[9])
            ),
        )


        val ethereumClient = createEthereumClient(allBlocks, expectedLogs)

        val descriptor = mockk<EthereumDescriptor>()
        every { descriptor.contracts } returns emptyList()
        every { descriptor.ethTopic } returns randomWord()

        val fullBlocks = ethereumClient.getBlockLogs(descriptor, listOf(EthereumBlockchainBlock(allBlocks[0])), true).toList()
        assertThat(fullBlocks).hasSameSizeAs(expectedFullBlocks)
        for ((block, logs) in fullBlocks) {
            val expectedFullBlock = expectedFullBlocks.find { it.block == block }!!
            assertThat(logs.map { it.index }).isEqualTo(expectedFullBlock.logs.map { it.index })
        }
    }

    @Suppress("ReactiveStreamsUnusedPublisher")
    private fun createEthereumClient(
        allBlocks: List<Block<Transaction>>,
        logs: List<EthereumBlockchainLog>
    ): EthereumClient {
        val monoEthereum = mockk<MonoEthereum>()
        every { monoEthereum.ethGetLogsJava(any()) } returns logs.map { it.ethLog }.toMono()
        every { monoEthereum.ethGetFullBlockByHash(any()) } answers {
            val blockHash = firstArg<Word>()
            val block = allBlocks.find { it.hash() == blockHash } ?: return@answers null
            val transactions = logs.filter {
                it.ethTransaction.blockNumber() == block.blockNumber
                        && it.ethTransaction.blockHash() == blockHash
            }.map { it.ethTransaction }
            Block(
                block.number(),
                block.hash(),
                block.parentHash(),
                block.nonce(),
                block.sha3Uncles(),
                block.logsBloom(),
                block.transactionsRoot(),
                block.stateRoot(),
                block.miner(),
                block.difficulty(),
                block.totalDifficulty(),
                block.extraData(),
                block.size(),
                block.gasLimit(),
                block.gasUsed(),
                CollectionConverters.asScala(transactions).toList(),
                block.timestamp()
            ).toMono()
        }

        val ethPubSub = mockk<EthPubSub>()
        every { ethPubSub.newHeads() } returns Flux.empty()
        val properties = EthereumScannerProperties()

        return EthereumClient(monoEthereum, properties, ethPubSub, monitor)
    }
}
