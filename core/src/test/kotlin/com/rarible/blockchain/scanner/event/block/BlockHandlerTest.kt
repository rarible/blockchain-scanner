package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomString
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.service.TestBlockService
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

internal class BlockHandlerTest {
    private val blockMapper = TestBlockMapper()
    private val blockClient =  mockk<TestBlockchainClient>()
    private val blockService = mockk<TestBlockService>()
    private val blockListener = mockk<BlockEventPublisher>()
    private val batchLoad = BlockBatchLoadProperties(
        enabled = true,
        batchSize = 3,
        confirmationBlockDistance = 10
    )
    private val blockHandler = BlockHandler(
        blockMapper,
        blockClient,
        blockService,
        blockListener,
        batchLoad
    )

    @Test
    fun `should batch load all old blocks`() = runBlocking {
        val lastStateBlockNumber = 1L
        val latestBlockNumber = 200L

        val testBlockchainBlocks = (lastStateBlockNumber.. latestBlockNumber)
            .map { createTestBlockchainBlock(it) }
            .let { makeChain(it) }
            .associateBy { it.number }
        val testBlocks = testBlockchainBlocks
            .mapValues { blockMapper.map(it.value) }

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
        } returns Unit

        blockHandler.onNewBlock(requireNotNull(testBlockchainBlocks[latestBlockNumber]))

        coVerify(exactly = (latestBlockNumber - lastStateBlockNumber).toInt()) {
            blockListener.publish(any())
        }
        coVerifyOrder {
            ((lastStateBlockNumber + 1)..latestBlockNumber).forEach { id ->
                val block = requireNotNull(testBlocks[id])
                blockListener.publish(NewBlockEvent(Source.BLOCKCHAIN, block.id, block.hash))
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
        return TestBlockchainBlock(
            TestOriginalBlock(
                number = number,
                hash = randomBlockHash(),
                parentHash = randomBlockHash(),
                timestamp = number,
                testExtra = randomString()
            )
        )
    }

    private fun makeChain(orderedBlocks: List<TestBlockchainBlock>): List<TestBlockchainBlock> {
        val first = orderedBlocks.first()
        val blockMap = orderedBlocks.associateBy { it.number }

        return orderedBlocks.map { block ->
            if (first == block) {
                first
            } else {
                val original = block.testOriginalBlock
                val parentHash = requireNotNull(blockMap[original.number - 1]).hash
                TestBlockchainBlock(original.copy(parentHash = parentHash))
            }
        }
    }
}
