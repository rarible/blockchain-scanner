package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.ethLog
import com.rarible.blockchain.scanner.ethereum.test.data.ethTransaction
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.core.common.justOrEmpty
import com.rarible.core.test.data.randomAddress
import io.daonomic.rpc.domain.Word
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

class EthereumBlockchainLogIndexTest {
    @Test
    fun `index is calculated in group of transactionHash, topic, address`() = runBlocking<Unit> {
        val allBlocks = listOf(
            ethBlock(),
            ethBlock(),
            ethBlock(),
            ethBlock(),
            ethBlock(),
            ethBlock()
        )

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
            block: Block<Word>
        ) = EthereumBlockchainLog(
            ethLog = ethLog(
                transactionHash = transactionHash,
                topic = topic,
                address = address,
                logIndex = logIndex,
                blockHash = block.hash()
            ),
            ethTransaction = ethTransaction(
                transactionHash = transactionHash,
                blockHash = block.hash(),
                blockNumber = block.number()
            ),
            index = index
        )

        val expectedLogs = listOf(
            /* 0 */
            createEthereumBL(0, randomWord(), topic1, randomAddress(), 0, allBlocks[0]),
            /* 1 */
            createEthereumBL(0, randomWord(), randomWord(), address1, 1, allBlocks[1]),
            /* 2 */
            createEthereumBL(0, randomWord(), topic1, address1, 2, allBlocks[2]),
            /* 3 */
            createEthereumBL(0, randomWord(), randomWord(), address2, 3, allBlocks[3]),

            // Group #1 of <transactionHash, topic, address>
            /* 4 */
            createEthereumBL(0, transactionHash1, topic1, address1, 4, allBlocks[4]),
            /* 5 */
            createEthereumBL(1, transactionHash1, topic1, address1, 5, allBlocks[4]),
            /* 6 */
            createEthereumBL(2, transactionHash1, topic1, address1, 6, allBlocks[4]),

            // Group #2 of <transactionHash, topic, address>
            /* 7 */
            createEthereumBL(0, transactionHash2, topic2, address2, 7, allBlocks[5]),
            /* 8 */
            createEthereumBL(1, transactionHash2, topic2, address2, 8, allBlocks[5]),
            /* 9 */
            createEthereumBL(2, transactionHash2, topic2, address2, 9, allBlocks[5]),
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

        val fullBlocks = ethereumClient.getBlockLogs(descriptor, LongRange(1, 1)).toList()
        assertThat(fullBlocks).hasSameSizeAs(expectedFullBlocks)
        for ((block, logs) in fullBlocks) {
            val expectedFullBlock = expectedFullBlocks.find { it.block == block }!!
            assertThat(logs.map { it.index }).isEqualTo(expectedFullBlock.logs.map { it.index })
        }
    }

    @Suppress("ReactiveStreamsUnusedPublisher")
    private fun createEthereumClient(
        allBlocks: List<Block<Word>>,
        logs: List<EthereumBlockchainLog>
    ): EthereumClient {
        val monoEthereum = mockk<MonoEthereum>()
        every { monoEthereum.ethGetLogsJava(any()) } returns logs.map { it.ethLog }.toMono()
        every { monoEthereum.ethGetBlockByHash(any()) } answers {
            val blockHash = firstArg<Word>()
            allBlocks.find { it.hash() == blockHash }.justOrEmpty()
        }
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

        return EthereumClient(monoEthereum, ethPubSub)
    }
}
