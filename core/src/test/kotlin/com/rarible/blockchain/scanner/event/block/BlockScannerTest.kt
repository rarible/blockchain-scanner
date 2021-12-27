package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

//todo test when block is PENDING - need to reprocess
class BlockScannerTest {

    private val client = mockk<BlockchainBlockClient<TestBlockchainBlock>>()
    private val service = mockk<BlockService>()

    private val scanner = BlockScanner(
        client,
        service,
        ScanRetryPolicyProperties(reconnectAttempts = 1),
        BlockBatchLoadProperties()
    )

    @BeforeEach
    fun beforeEach() {
        clearMocks(client, service)
    }

    @Test
    fun `first event, state is empty`() = runBlocking {
        val block0 = randomBlockchainBlock("block0", 0, null)
        val block1 = randomBlockchainBlock("block1", 1, "block0")

        every { client.newBlocks } returns flowOf(block1)
        // We expect both blocks will be requested from the client
        coEvery { client.getBlock(0) } returns block0
        coEvery { client.getBlock(1) } returns block1

        // Imitating empty DB
        coEvery { service.getBlock(any()) } returns null
        coEvery { service.getLastBlock() } returns null
        coEvery { service.save(any()) } answers { it.invocation.args.first() as Block }

        val events = scanOnce()

        // Events for both blocks should be emitted in correct order
        assertThat(events).isEqualTo(
            listOf(
                NewBlockEvent(Source.BLOCKCHAIN, block0.number, block0.hash),
                NewBlockEvent(Source.BLOCKCHAIN, block1.number, block1.hash)
            )
        )

        // Both blocks should be saved in DB
        val stateBlock0 = block0.toBlock()
        coVerify(exactly = 1) { service.save(stateBlock0) }
        coVerify(exactly = 1) { service.save(stateBlock0.copy(status = BlockStatus.SUCCESS)) }
        val stateBlock1 = block1.toBlock()
        coVerify(exactly = 1) { service.save(stateBlock1) }
        coVerify(exactly = 1) { service.save(stateBlock1.copy(status = BlockStatus.SUCCESS)) }

        // Last block should be requested from state only once
        coVerify(exactly = 1) { service.getLastBlock() }
        // Parent (root in current case) should be requested only once
        coVerify(exactly = 1) { client.getBlock(0) }
        // We don't need to call here getBlock because we just received it in event
        coVerify(exactly = 0) { client.getBlock(1) }
        verify(exactly = 1) { client.newBlocks }
        confirmVerified(client, service)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `root block in DB, one block is missing in DB`(success: Boolean) = runBlocking {
        val block0 = randomBlockchainBlock("block0", 0, null)
        val block1 = randomBlockchainBlock("block1", 1, "block0")
        val block2 = randomBlockchainBlock("block2", 2, "block1")

        // Imitates event for block2 while block0 already in DB, but block1 is missing
        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlock(0) } returns block0
        coEvery { client.getBlock(1) } returns block1
        coEvery { client.getBlock(2) } returns block2

        // We already have root block in DB
        coEvery { service.getLastBlock() } returns block0.toBlock().copy(status = if (success) BlockStatus.SUCCESS else BlockStatus.PENDING)
        coEvery { service.save(any()) } answers { it.invocation.args.first() as Block }

        val events = scanOnce()

        // Events for both blocks (1 and 2) should be emitted in correct order
        val requiredEvents = listOf(
            NewBlockEvent(Source.BLOCKCHAIN, block1.number, block1.hash),
            NewBlockEvent(Source.BLOCKCHAIN, block2.number, block2.hash)
        )
        assertThat(events).isEqualTo(
            if (success)
                requiredEvents
            else
                listOf(NewBlockEvent(Source.BLOCKCHAIN, block0.number, block0.hash)) + requiredEvents
        )

        coVerify(exactly = 1) { client.getBlock(0) }
        coVerify(exactly = 1) { client.getBlock(1) }
        coVerify(exactly = 1) { client.getBlock(2) }

        coVerify(exactly = 1) { service.getLastBlock() }

        val stateBlock1 = block1.toBlock()
        if (!success) {
            val stateBlock0 = block0.toBlock()
            coVerify(exactly = 1) { service.save(stateBlock0) }
            coVerify(exactly = 1) { service.save(stateBlock0.copy(status = BlockStatus.SUCCESS)) }
        }
        coVerify(exactly = 1) { service.save(stateBlock1) }
        coVerify(exactly = 1) { service.save(stateBlock1.copy(status = BlockStatus.SUCCESS)) }
        val stateBlock2 = block2.toBlock()
        coVerify(exactly = 1) { service.save(stateBlock2) }
        coVerify(exactly = 1) { service.save(stateBlock2.copy(status = BlockStatus.SUCCESS)) }

        verify { client.newBlocks }
        confirmVerified(client, service)
    }

    @Test
    fun `chain reorganized`() = runBlocking {
        val block0 = randomBlockchainBlock("block0", 0, null)
        val block1Reorg = randomBlockchainBlock("block1-reorg", 1, "block0")
        val block2Reorg = randomBlockchainBlock("block2-reorg", 2, "block1-reorg")
        val block1 = randomBlockchainBlock("block1", 1, "block0")
        val block2 = randomBlockchainBlock("block2", 2, "block1")
        val block3 = randomBlockchainBlock("block3", 3, "block2")

        every { client.newBlocks } returns flowOf(block3)
        coEvery { client.getBlock(0) } returns block0
        coEvery { client.getBlock(1) } returns block1
        coEvery { client.getBlock(2) } returns block2
        coEvery { client.getBlock(3) } returns block3

        // We have in DB correct root block, block1 and block2 pretends to be reverted
        coEvery { service.getBlock(0) } returns block0.toBlock().copy(status = BlockStatus.SUCCESS)
        coEvery { service.getBlock(1) } returns block1Reorg.toBlock()
        coEvery { service.getLastBlock() } returns block2Reorg.toBlock()

        coEvery { service.save(any()) } answers { it.invocation.args.first() as Block }
        coEvery { service.remove(any()) } answers {}

        val events = scanOnce()
        // Reverted events should be emitted first in DESC order
        // Correct events should be emitted in ASC order right after reverted block events
        assertThat(events).isEqualTo(
            listOf(
                RevertedBlockEvent(Source.BLOCKCHAIN, block2Reorg.number, block2Reorg.hash),
                RevertedBlockEvent(Source.BLOCKCHAIN, block1Reorg.number, block1Reorg.hash),
                NewBlockEvent(Source.BLOCKCHAIN, block1.number, block1.hash),
                NewBlockEvent(Source.BLOCKCHAIN, block2.number, block2.hash),
                NewBlockEvent(Source.BLOCKCHAIN, block3.number, block3.hash),
            )
        )

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlock(0) }
        coVerify(exactly = 2) { client.getBlock(1) }
        coVerify(exactly = 2) { client.getBlock(2) }
        coVerify(exactly = 1) { client.getBlock(3) }

        coVerify(exactly = 1) { service.getLastBlock() }
        coVerify(exactly = 1) { service.getBlock(0) }
        coVerify(exactly = 1) { service.getBlock(1) }
        coVerify(exactly = 1) { service.save(block1.toBlock()) }
        coVerify(exactly = 1) { service.save(block1.toBlock().copy(status = BlockStatus.SUCCESS)) }
        coVerify(exactly = 1) { service.save(block2.toBlock()) }
        coVerify(exactly = 1) { service.save(block2.toBlock().copy(status = BlockStatus.SUCCESS)) }
        coVerify(exactly = 1) { service.save(block3.toBlock()) }
        coVerify(exactly = 1) { service.save(block3.toBlock().copy(status = BlockStatus.SUCCESS)) }
        coVerify(exactly = 1) { service.remove(block1Reorg.number) }
        coVerify(exactly = 1) { service.remove(block2Reorg.number) }
        confirmVerified(client, service)
    }

    @Test
    fun `reorg happens after start`() = runBlocking {
        val block0 = randomBlockchainBlock("block0", 0, null)
        val block1 = randomBlockchainBlock("block1", 1, "block0")
        val block2 = randomBlockchainBlock("block2", 2, "block1-new")
        val block3 = randomBlockchainBlock("block3", 3, "block2-new")

        every { client.newBlocks } returns flowOf(block3)
        // For block 1 we received same data as data in DB
        coEvery { client.getBlock(1) } returns block1
        // But block 2 has been changed while we were restoring chain
        coEvery { client.getBlock(2) } returns block2
        coEvery { client.getBlock(3) } returns block3

        // We have only 2 blocks in DB
        coEvery { service.getBlock(0) } returns block0.toBlock()
        coEvery { service.getLastBlock() } returns block1.toBlock().copy(status = BlockStatus.SUCCESS)
        coEvery { service.save(any()) } answers { it.invocation.args.first() as Block }
        coEvery { service.remove(any()) } answers { }

        val events = scanOnce()

        // Nothing should be emitted here since chain reorganized, all event will be emitted at next block event
        assertThat(events).isEqualTo(emptyList<BlockEvent>())

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlock(1) }
        coVerify(exactly = 1) { client.getBlock(2) }
        // There is no calls for block 3 because we found chain has been reorganized at block 2
        coVerify(exactly = 0) { client.getBlock(3) }

        coVerify(exactly = 1) { service.getLastBlock() }
        confirmVerified(client, service)
    }

    private class CollectionBlockEventPublisher : BlockEventPublisher {

        val events = mutableListOf<BlockEvent>()

        override suspend fun publish(event: BlockEvent) {
            events.add(event)
        }
    }

    private suspend fun scanOnce(): List<BlockEvent> {
        val listener = CollectionBlockEventPublisher()
        try {
            scanner.scan(listener)
        } catch (e: IllegalStateException) {
            // Do nothing, in prod there will be infinite attempts count
        }
        return listener.events
    }
}
