package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.data.randomInt
import com.rarible.blockchain.scanner.ethereum.test.data.randomPositiveBigInt
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.core.common.justOrEmpty
import com.rarible.core.test.data.randomAddress
import io.daonomic.rpc.domain.Binary
import io.daonomic.rpc.domain.Word
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toMono
import scala.jdk.javaapi.CollectionConverters
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.domain.response.Block
import scalether.domain.response.Log
import java.math.BigInteger

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
        val block1 = allBlocks[0]

        val transactionHash2 = randomWord()
        val topic2 = randomWord()
        val address2 = randomAddress()
        val block2 = allBlocks[1]

        val expectedLogs = listOf(
            IndexedValue(0, ethLog(transactionHash1, topic1, randomAddress(), 0, allBlocks[0].hash())),
            IndexedValue(0, ethLog(transactionHash1, randomWord(), address1, 1, allBlocks[1].hash())),
            IndexedValue(0, ethLog(randomWord(), topic1, address1, 2, allBlocks[2].hash())),
            IndexedValue(0, ethLog(randomWord(), randomWord(), randomAddress(), 3, allBlocks[3].hash())),

            // Group #1 of <transactionHash, topic, address>
            IndexedValue(0, ethLog(transactionHash1, topic1, address1, 4, allBlocks[4].hash())),
            IndexedValue(1, ethLog(transactionHash1, topic1, address1, 5, allBlocks[4].hash())),
            IndexedValue(2, ethLog(transactionHash1, topic1, address1, 6, allBlocks[4].hash())),

            // Group #2 of <transactionHash, topic, address>
            IndexedValue(0, ethLog(transactionHash2, topic2, address2, 7, allBlocks[5].hash())),
            IndexedValue(1, ethLog(transactionHash2, topic2, address2, 8, allBlocks[5].hash())),
            IndexedValue(2, ethLog(transactionHash2, topic2, address2, 9, allBlocks[5].hash())),
        )

        val monoEthereum = mockk<MonoEthereum>()
        every { monoEthereum.ethGetLogsJava(any()) } returns expectedLogs.map { it.value }.toMono()
        every { monoEthereum.ethGetBlockByHash(any()) } answers {
            val blockHash = firstArg<Word>()
            allBlocks.find { it.hash() == blockHash }.justOrEmpty()
        }
        every { monoEthereum.ethGetBlockByHash(block1.hash()) } returns block1.toMono()
        every { monoEthereum.ethGetBlockByHash(block2.hash()) } returns block2.toMono()

        val ethPubSub = mockk<EthPubSub>()
        every { ethPubSub.newHeads() } returns Flux.empty()

        val client = EthereumClient(monoEthereum, ethPubSub)

        val descriptor = mockk<EthereumDescriptor>()
        every { descriptor.contracts } returns emptyList()
        every { descriptor.ethTopic } returns randomWord()

        val fullBlocks = client.getBlockLogs(descriptor, LongRange(1, 1)).toList()
        assertEquals(6, fullBlocks.size)
        fullBlocks.forEach { println(it) }
        for ((expectedIndex, log) in expectedLogs) {
            val fullBlock = fullBlocks.find { it.block.hash == log.blockHash().toString() }
            assertNotNull(fullBlock) { "No full block found for block hash ${log.blockHash()}" }

            val ethereumLog = fullBlock!!.logs.find { it.ethLog == log }
            assertNotNull(ethereumLog) { "No log returned for $log" }
            assertEquals(expectedIndex, ethereumLog!!.index)
        }
    }

    private fun ethLog(
        hash: Word,
        topic: Word,
        address: Address,
        logIndex: Int,
        blockHash: Word
    ) = Log(
        logIndex.toBigInteger(),
        randomPositiveBigInt(100),
        hash,
        blockHash,
        randomPositiveBigInt(100),
        address,
        Binary.apply("0x0"),
        CollectionConverters.asScala(listOf(topic)).toList(),
        randomString()
    )

    private fun ethBlock() = ethBlock(randomInt(), randomWord())
    private fun ethBlock(number: Int, hash: Word): Block<Word> = Block<Word>(
        number.toBigInteger(),
        hash,
        randomWord(),
        randomString(),
        randomString(),
        randomString(),
        randomString(),
        randomString(),
        randomAddress(),
        randomPositiveBigInt(1),
        randomPositiveBigInt(1),
        Binary.empty(),
        BigInteger.ZERO,
        BigInteger.ZERO,
        BigInteger.ZERO,
        CollectionConverters.asScala(emptyList<Word>()).toList(),
        BigInteger.ZERO,
    )
}
