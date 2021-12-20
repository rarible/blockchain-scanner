package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.model.TestBlock
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
internal class BlockScannerIt : AbstractIntegrationTest() {

    private var blockEventPublisher: BlockEventPublisher = mockk()

    @BeforeEach
    fun beforeEach() {
        clearMocks(blockEventPublisher)
        coEvery { blockEventPublisher.publish(any()) } returns Unit
    }

    @Test
    fun `block event - first block received`() = runBlocking<Unit> {
        val block = randomOriginalBlock().copy(number = 0)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(block),
            newBlocks = listOf(block)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        // New block saved, listener notified with single event
        val savedBlock = findBlock(block.number)
        assertThat(savedBlock).isEqualTo(
            testBlockMapper.map(TestBlockchainBlock(block))
        )
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    source = Source.BLOCKCHAIN,
                    number = 0,
                    hash = block.hash
                )
            )
        }
    }

    @Test
    fun `block event - new block received, has stored parent`() = runBlocking {
        val existingBlock = saveBlock(randomOriginalBlock(5))
        val newBlock = randomOriginalBlock(6).copy(parentHash = existingBlock.hash)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock, newBlock),
            newBlocks = listOf(newBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        val savedNewBlock = findBlock(newBlock.number)

        // New block saved, listener notified with single event existing block should not emit event
        assertThat(savedNewBlock).isEqualTo(testBlockMapper.map(TestBlockchainBlock(newBlock)))
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(Source.BLOCKCHAIN, newBlock.number, newBlock.hash)
            )
        }
        coVerify(exactly = 0) {
            blockEventPublisher.publish(
                NewBlockEvent(Source.BLOCKCHAIN, existingBlock.number, existingBlock.hash)
            )
        }
        confirmVerified(blockEventPublisher)
    }

    @Test
    fun `block event - new block received, last known block is far away`() = runBlocking {
        val firstBlock = saveBlock(randomOriginalBlock(0))
        val existingBlock = saveBlock(randomOriginalBlock(1)).copy(parentHash = firstBlock.hash)
        val missedBlock = randomOriginalBlock(2).copy(parentHash = existingBlock.hash)
        val newBlock = randomOriginalBlock(3).copy(parentHash = missedBlock.hash)
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(firstBlock, existingBlock, missedBlock, newBlock),
            newBlocks = listOf(newBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        // Missed block and new block saved, listener notified with 2 events
        assertThat(findBlock(missedBlock.number)).isEqualTo(testBlockMapper.map(TestBlockchainBlock(missedBlock)))
        assertThat(findBlock(newBlock.number)).isEqualTo(testBlockMapper.map(TestBlockchainBlock(newBlock)))

        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(source = Source.BLOCKCHAIN, number = newBlock.number, hash = newBlock.hash)
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(source = Source.BLOCKCHAIN, number = missedBlock.number, hash = missedBlock.hash)
            )
        }
        coVerify(exactly = 0) {
            blockEventPublisher.publish(
                NewBlockEvent(source = Source.BLOCKCHAIN, number = existingBlock.number, hash = existingBlock.hash)
            )
        }
        confirmVerified(blockEventPublisher)
    }

    @Test
    fun `block event - new block received, block chain changed`() = runBlocking {
        // Data we have in storage
        val existingRoot = saveBlock(randomOriginalBlock(0))
        val existingGrandParent = saveBlock(randomOriginalBlock(1).copy(parentHash = existingRoot.hash))
        val existingParent = saveBlock(randomOriginalBlock(2).copy(parentHash = existingGrandParent.hash))

        // Root block #3 is the same, #4 and #5 were changed
        val newGrandParent = randomOriginalBlock(1).copy(parentHash = existingRoot.hash)
        val newParent = randomOriginalBlock(2).copy(parentHash = newGrandParent.hash)

        // New block refers to the newParent in Blockchain
        val newBlock = randomOriginalBlock(3).copy(parentHash = newParent.hash)

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
            testBlockMapper.map(TestBlockchainBlock(existingRoot))
        )
        assertThat(savedNewGrandparent).isEqualTo(
            testBlockMapper.map(TestBlockchainBlock(newGrandParent))
        )
        assertThat(savedNewParent).isEqualTo(
            testBlockMapper.map(TestBlockchainBlock(newParent))
        )
        assertThat(savedNewBlock).isEqualTo(
            testBlockMapper.map(TestBlockchainBlock(newBlock))
        )

        // Changed blocks should emit new events along with new block event, event for root block should not be emitted
        // reverted block events should contain metadata of reverted blocks
        coVerify(exactly = 5) { blockEventPublisher.publish(any()) }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                RevertedBlockEvent(
                    Source.BLOCKCHAIN,
                    existingGrandParent.number,
                    existingGrandParent.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                RevertedBlockEvent(
                    Source.BLOCKCHAIN,
                    existingParent.number,
                    existingParent.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    Source.BLOCKCHAIN,
                    newBlock.number,
                    newBlock.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    Source.BLOCKCHAIN,
                    newParent.number,
                    newParent.hash
                )
            )
        }
        coVerify(exactly = 1) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    Source.BLOCKCHAIN,
                    newGrandParent.number,
                    newGrandParent.hash
                )
            )
        }
        coVerify(exactly = 0) {
            blockEventPublisher.publish(
                NewBlockEvent(
                    Source.BLOCKCHAIN,
                    existingRoot.number,
                    existingRoot.hash
                )
            )
        }
    }

    @Test
    fun `block event - existing block received`() = runBlocking {
        val existingBlock = saveBlock(randomOriginalBlock(4))
        val testBlockchainData = TestBlockchainData(
            blocks = listOf(existingBlock),
            newBlocks = listOf(existingBlock)
        )

        val blockScanner = createBlockScanner(testBlockchainData)
        blockScanner.scanOnce(blockEventPublisher)

        val storedExistingBlock = findBlock(existingBlock.number)!!

        // Existing block should not be changed, no events should be emitted
        assertThat(storedExistingBlock).isEqualTo(
            testBlockMapper.map(TestBlockchainBlock(existingBlock))
        )
        coVerify(exactly = 0) { blockEventPublisher.publish(any()) }
    }

    private fun createBlockScanner(
        testBlockchainData: TestBlockchainData
    ): BlockScanner<TestBlockchainBlock, TestBlock> {
        return BlockScanner(
            testBlockMapper,
            TestBlockchainClient(testBlockchainData),
            testBlockService,
            properties.retryPolicy.scan
        )
    }
}
