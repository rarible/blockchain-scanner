package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.configuration.TestBlockchainScannerProperties
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.assertOriginalBlockAndBlockEquals
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import com.rarible.blockchain.scanner.test.service.TestBlockService
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
internal class BlockScannerIt {

    @Autowired
    lateinit var testBlockService: TestBlockService

    @Autowired
    lateinit var testBlockMapper: TestBlockMapper

    @Autowired
    lateinit var testBlockRepository: TestBlockRepository

    @Autowired
    lateinit var properties: TestBlockchainScannerProperties

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

        scanOnce(createBlockScanner(testBlockchainData))

        val savedBlock = testBlockRepository.findById(block.number)

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

        scanOnce(createBlockScanner(testBlockchainData))

        val savedNewBlock = testBlockRepository.findById(newBlock.number)

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

        scanOnce(createBlockScanner(testBlockchainData))

        val savedNewBlock = testBlockRepository.findById(newBlock.number)

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

        scanOnce(createBlockScanner(testBlockchainData))

        val savedRoot = testBlockRepository.findById(existingRoot.number)!!
        val savedNewGrandparent = testBlockRepository.findById(newGrandParent.number)!!
        val savedNewParent = testBlockRepository.findById(newParent.number)!!
        val savedNewBlock = testBlockRepository.findById(newBlock.number)!!

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

        scanOnce(createBlockScanner(testBlockchainData))

        val storedExistingBlock = testBlockRepository.findById(existingBlock.number)!!

        // Existing block should not be changed, no events should be emitted
        assertOriginalBlockAndBlockEquals(existingBlock, storedExistingBlock)
        assertEquals(Block.Status.SUCCESS, storedExistingBlock.status)
        coVerify(exactly = 0) { blockListener.onBlockEvent(any()) }
    }


    private fun createBlockScanner(
        testBlockchainData: TestBlockchainData
    ): BlockScanner<TestBlockchainBlock, TestBlockchainLog, TestBlock, TestDescriptor> {
        return BlockScanner(
            TestBlockchainClient(testBlockchainData),
            testBlockMapper,
            testBlockService,
            properties
        )
    }

    private suspend fun scanOnce(blockScanner: BlockScanner<*, *, *, *>) {
        try {
            blockScanner.scan(blockListener)
        } catch (e: IllegalStateException) {
            // Do nothing, in prod there will be infinite attempts count
        }
    }

    private fun blockEvent(block: TestOriginalBlock, reverted: TestOriginalBlock? = null): BlockEvent {
        return BlockEvent(
            Source.BLOCKCHAIN,
            TestBlockchainBlock(block).meta,
            reverted?.let { TestBlockchainBlock(reverted).meta }
        )
    }

    private suspend fun saveBlock(
        block: TestOriginalBlock,
        status: Block.Status = Block.Status.SUCCESS
    ): TestOriginalBlock {
        testBlockRepository.save(testBlockMapper.map(TestBlockchainBlock(block)).copy(status = status))
        return block
    }

}