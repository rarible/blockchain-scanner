package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

internal class BlockHandlerTest {
    private val blockClient = mockk<TestBlockchainClient>()
    private val blockService = mockk<BlockService>()
    private val blockListener = mockk<BlockEventPublisher>()
    private val batchLoad = BlockBatchLoadProperties(
        enabled = true,
        batchSize = 3,
        confirmationBlockDistance = 10
    )
    private val blockHandler = BlockHandler(
        blockClient,
        blockService,
        blockListener,
        batchLoad
    )

    @Test
    fun `should batch load all old blocks`() = runBlocking {
        val lastStateBlockNumber = 1L
        val latestBlockNumber = 200L

        val testBlockchainBlocks = (lastStateBlockNumber..latestBlockNumber)
            .map { createTestBlockchainBlock(it) }
            .let { makeChain(it) }
            .associateBy { it.number }
        val testBlocks = testBlockchainBlocks
            .mapValues { it.value.toBlock() }

        coEvery { blockService.getLastBlock() } returns testBlocks[lastStateBlockNumber]

        coEvery {
            blockClient.getBlock(any())
        } answers {
            val id = it.invocation.args.first() as Long
            requireNotNull(testBlockchainBlocks[id])
        }

        coEvery {
            blockListener.publish(any())
        } returns Unit

        coEvery {
            blockService.save(any())
        } answers { it.invocation.args.first() as Block }

        blockHandler.onNewBlock(requireNotNull(testBlockchainBlocks[latestBlockNumber]))

        coVerify(exactly = (latestBlockNumber - lastStateBlockNumber).toInt()) {
            blockListener.publish(any())
        }
        coVerifyOrder {
            ((lastStateBlockNumber + 1)..latestBlockNumber).forEach { id ->
                val block = requireNotNull(testBlocks[id])
                blockListener.publish(NewBlockEvent(block.id, block.hash))
            }
        }

        coVerify(exactly = (latestBlockNumber - lastStateBlockNumber).toInt()) {
            blockService.save(any())
        }
        coVerifyOrder {
            ((lastStateBlockNumber + 1)..latestBlockNumber).forEach { id ->
                val block = requireNotNull(testBlocks[id])
                blockService.save(block)
            }
        }
    }

    private fun createTestBlockchainBlock(number: Long): TestBlockchainBlock {
        return randomBlockchainBlock(
            hash = randomBlockHash(),
            number = number,
            parentHash = randomBlockHash()
        )
    }

    private fun makeChain(orderedBlocks: List<TestBlockchainBlock>): List<TestBlockchainBlock> {
        val first = orderedBlocks.first()
        val blockMap = orderedBlocks.associateBy { it.number }

        return orderedBlocks.map { block ->
            if (first == block) {
                first
            } else {
                val parentHash = requireNotNull(blockMap[block.number - 1]).hash
                block.copy(parentHash = parentHash)
            }
        }
    }
}
