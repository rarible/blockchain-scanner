package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@IntegrationTest
class BlockScannerIt : AbstractIntegrationTest() {

    private var blockEventPublisher: BlockEventPublisher = mockk()

    @BeforeEach
    fun beforeEach() {
        clearMocks(blockEventPublisher)
        coEvery { blockEventPublisher.publish(any()) } returns Unit
    }

    @Test
    fun `block event - first block received`() = runBlocking {
        val block = randomBlockchainBlock(number = 0)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(block),
            newBlocks = listOf(block)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        // New block saved, listener notified with single event
        val savedBlock = findBlock(block.number)
        assertThat(savedBlock).isEqualTo(
            block.toBlock()
        )
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    number = 0,
                    hash = block.hash
                )
            )
        }
    }

    @Test
    fun `block event - new block received, has stored parent`() = runBlocking {
        val existingBlock = saveBlock(randomBlockchainBlock(number = 5))
        val newBlock = randomBlockchainBlock(hash = randomBlockHash(), number = 6, parentHash = existingBlock.hash)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock, newBlock),
            newBlocks = listOf(newBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        val savedNewBlock = findBlock(newBlock.number)

        // New block saved, listener notified with single event existing block should not emit event
        assertThat(savedNewBlock).isEqualTo(newBlock.toBlock())
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(newBlock.number, newBlock.hash)
            )
        }
        coVerify(exactly = 0) {
            blockEventPublisher.publish(
                NewBlockEvent(existingBlock.number, existingBlock.hash)
            )
        }
        confirmVerified(blockEventPublisher)
    }

    @Test
    fun `block event - new block received, last known block is far away`() = runBlocking {
        val firstBlock = saveBlock(randomBlockchainBlock(number = 0))
        val existingBlock = saveBlock(randomBlockchainBlock(number = 1, parentHash = firstBlock.hash))
        val missedBlock = randomBlockchainBlock(number = 2, parentHash = existingBlock.hash)
        val newBlock = randomBlockchainBlock(number = 3, parentHash = missedBlock.hash)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(firstBlock, existingBlock, missedBlock, newBlock),
            newBlocks = listOf(newBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        // Missed block and new block saved, listener notified with 2 events
        assertThat(findBlock(missedBlock.number)).isEqualTo(missedBlock.toBlock())
        assertThat(findBlock(newBlock.number)).isEqualTo(newBlock.toBlock())

        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(number = newBlock.number, hash = newBlock.hash)
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(number = missedBlock.number, hash = missedBlock.hash)
            )
        }
        coVerify(exactly = 0) {
            blockEventPublisher.publish(
                NewBlockEvent(number = existingBlock.number, hash = existingBlock.hash)
            )
        }
        confirmVerified(blockEventPublisher)
    }

    @Test
    fun `block event - new block received, block chain changed`() = runBlocking {
        // Data we have in storage
        val existingRoot = saveBlock(randomBlockchainBlock(number = 0))
        val existingGrandParent = saveBlock(randomBlockchainBlock(number = 1, parentHash = existingRoot.hash))
        val existingParent = saveBlock(randomBlockchainBlock(number = 2, parentHash = existingGrandParent.hash))

        // Root block #3 is the same, #4 and #5 were changed
        val newGrandParent = randomBlockchainBlock(number = 1, parentHash = existingRoot.hash)
        val newParent = randomBlockchainBlock(number = 2, parentHash = newGrandParent.hash)

        // New block refers to the newParent in Blockchain
        val newBlock = randomBlockchainBlock(number = 3, parentHash = newParent.hash)

        val testBlockchainData = TestBlockchainData(
            blocks = listOf(newBlock, newParent, newGrandParent, existingRoot),
            newBlocks = listOf(newBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        val savedRoot = findBlock(existingRoot.number)!!
        val savedNewGrandparent = findBlock(newGrandParent.number)!!
        val savedNewParent = findBlock(newParent.number)!!
        val savedNewBlock = findBlock(newBlock.number)!!

        // Now we need to ensure all changed blocks are stored in DB and root was not changed
        assertThat(savedRoot).isEqualTo(
            existingRoot.toBlock()
        )
        assertThat(savedNewGrandparent).isEqualTo(
            newGrandParent.toBlock()
        )
        assertThat(savedNewParent).isEqualTo(
            newParent.toBlock()
        )
        assertThat(savedNewBlock).isEqualTo(
            newBlock.toBlock()
        )

        // Changed blocks should emit new events along with new block event, event for root block should not be emitted
        // reverted block events should contain metadata of reverted blocks
        coVerify(exactly = 5) { blockEventPublisher.publish(any()) }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                RevertedBlockEvent(
                    number = existingGrandParent.number,
                    hash = existingGrandParent.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                RevertedBlockEvent(
                    number = existingParent.number,
                    hash = existingParent.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    newBlock.number,
                    newBlock.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    newParent.number,
                    newParent.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    newGrandParent.number,
                    newGrandParent.hash
                )
            )
        }
        coVerify(exactly = 0) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    existingRoot.number,
                    existingRoot.hash
                )
            )
        }
    }

    @Test
    fun `block event - existing block received`() = runBlocking {
        val existingBlock = saveBlock(randomBlockchainBlock(number = 4))
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock),
            newBlocks = listOf(existingBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        val storedExistingBlock = findBlock(existingBlock.number)!!

        // Existing block should not be changed, no events should be emitted
        assertThat(storedExistingBlock).isEqualTo(
            existingBlock.toBlock()
        )
        coVerify(exactly = 0) { blockEventPublisher.publish(any()) }
    }

    private fun createBlockScanner(
        testBlockchainData: TestBlockchainData
    ): BlockScanner<TestBlockchainBlock> {
        return BlockScanner(
            TestBlockchainClient(testBlockchainData),
            testBlockService,
            properties.retryPolicy.scan,
            properties.scan.blockConsume.batchLoad
        )
    }
}
