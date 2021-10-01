package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.metrics.Metrics
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.assertOriginalBlockAndBlockEquals
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
internal class BlockScannerIt : AbstractIntegrationTest() {

    private var blockListener: BlockListener = mockk()

    @BeforeEach
    fun beforeEach() {
        clearMocks(blockListener)
        coEvery { blockListener.onBlockEvent(any()) } returns Unit
    }

    @Test
    fun `block event - first block received`() = runBlocking {
        val block = randomOriginalBlock()
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(block),
            newBlocks = listOf(block)
        )

        scanOnce(createBlockScanner(testBlockchainData), blockListener)

        val savedBlock = findBlock(block.number)

        // New block saved with status PENDING, listener notified with single event
        assertOriginalBlockAndBlockEquals(block, savedBlock!!)
        assertEquals(Block.Status.PENDING, savedBlock.status)
        coVerify(exactly = 1) { blockListener.onBlockEvent(any()) }
    }

    @Test
    fun `block event - new block received, has stored parent`() = runBlocking {
        val existingBlock = saveBlock(randomOriginalBlock(5))
        val newBlock = randomOriginalBlock(6).copy(parentHash = existingBlock.hash)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock, newBlock),
            newBlocks = listOf(newBlock)
        )

        scanOnce(createBlockScanner(testBlockchainData), blockListener)

        val savedNewBlock = findBlock(newBlock.number)

        // New block saved with status PENDING, listener notified with single event
        // existing block should not emit event
        assertOriginalBlockAndBlockEquals(newBlock, savedNewBlock!!)
        assertEquals(Block.Status.PENDING, savedNewBlock.status)
        coVerify(exactly = 1) { blockListener.onBlockEvent(any()) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(newBlock)) }
        coVerify(exactly = 0) { blockListener.onBlockEvent(blockEvent(existingBlock)) }
    }

    @Test
    fun `block event - new block received, last known block is far away`() = runBlocking {
        val existingBlock = saveBlock(randomOriginalBlock(4))
        val missedBlock = randomOriginalBlock(5).copy(parentHash = existingBlock.parentHash)
        val newBlock = randomOriginalBlock(6).copy(parentHash = missedBlock.hash)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock, missedBlock, newBlock),
            newBlocks = listOf(newBlock)
        )

        scanOnce(createBlockScanner(testBlockchainData), blockListener)

        val savedNewBlock = findBlock(newBlock.number)

        // Missed block and new block saved with status PENDING, listener notified with 2 events
        assertOriginalBlockAndBlockEquals(newBlock, savedNewBlock!!)
        assertEquals(Block.Status.PENDING, savedNewBlock.status)
        coVerify(exactly = 2) { blockListener.onBlockEvent(any()) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(newBlock)) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(missedBlock)) }
        coVerify(exactly = 0) { blockListener.onBlockEvent(blockEvent(existingBlock)) }
    }

    @Test
    fun `block event - new block received, block chain changed`() = runBlocking {
        // Data we have in storage
        val existingRoot = saveBlock(randomOriginalBlock(3))
        val existingGrandParent = saveBlock(randomOriginalBlock(4).copy(parentHash = existingRoot.hash))
        val existingParent = saveBlock(randomOriginalBlock(5).copy(parentHash = existingGrandParent.hash))

        // Root block #3 is the same, #4 and #5 were changed
        val newGrandParent = randomOriginalBlock(4).copy(parentHash = existingRoot.hash)
        val newParent = randomOriginalBlock(5).copy(parentHash = newGrandParent.hash)

        // New block refers to the newParent in Blockchain
        val newBlock = randomOriginalBlock(6).copy(parentHash = newParent.hash)

        val testBlockchainData = TestBlockchainData(
            blocks = listOf(newBlock, newParent, newGrandParent, existingRoot),
            newBlocks = listOf(newBlock)
        )

        scanOnce(createBlockScanner(testBlockchainData), blockListener)

        val savedRoot = findBlock(existingRoot.number)!!
        val savedNewGrandparent = findBlock(newGrandParent.number)!!
        val savedNewParent = findBlock(newParent.number)!!
        val savedNewBlock = findBlock(newBlock.number)!!

        // Now we need to ensure all changed blocks are stored in DB and root was not changed
        assertOriginalBlockAndBlockEquals(existingRoot, savedRoot)
        assertEquals(Block.Status.SUCCESS, savedRoot.status)

        // Changed blocks should have status PENDING
        assertOriginalBlockAndBlockEquals(newGrandParent, savedNewGrandparent)
        assertEquals(Block.Status.PENDING, savedNewGrandparent.status)

        assertOriginalBlockAndBlockEquals(newParent, savedNewParent)
        assertEquals(Block.Status.PENDING, savedNewParent.status)

        assertOriginalBlockAndBlockEquals(newBlock, savedNewBlock)
        assertEquals(Block.Status.PENDING, savedNewBlock.status)

        // Changed blocks should emit new events along with new block event, event for root block should not be emitted
        // reverted block events should contain metadata of reverted blocks
        // TODO in reverted blocks there is timestamp of new block, not sure is this correct
        coVerify(exactly = 3) { blockListener.onBlockEvent(any()) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(newGrandParent, existingGrandParent)) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(newParent, existingParent)) }
        coVerify(exactly = 1) { blockListener.onBlockEvent(blockEvent(newBlock)) }
        coVerify(exactly = 0) { blockListener.onBlockEvent(blockEvent(existingRoot)) }
    }

    @Test
    fun `block event - existing block received`() = runBlocking {
        val existingBlock = saveBlock(randomOriginalBlock(4), Block.Status.SUCCESS)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock),
            newBlocks = listOf(existingBlock)
        )

        scanOnce(createBlockScanner(testBlockchainData), blockListener)

        val storedExistingBlock = findBlock(existingBlock.number)!!

        // Existing block should not be changed, no events should be emitted
        assertOriginalBlockAndBlockEquals(existingBlock, storedExistingBlock)
        assertEquals(Block.Status.SUCCESS, storedExistingBlock.status)
        coVerify(exactly = 0) { blockListener.onBlockEvent(any()) }
    }

    private fun createBlockScanner(
        testBlockchainData: TestBlockchainData
    ): BlockScanner<TestBlockchainBlock, TestBlockchainLog, TestBlock, TestDescriptor> {
        return BlockScanner(
            Metrics(null),
            TestBlockchainClient(testBlockchainData),
            testBlockMapper,
            testBlockService,
            properties.retryPolicy.scan,
            properties.blockBufferSize
        )
    }
}
