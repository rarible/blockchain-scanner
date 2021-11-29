package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.v2.BlockScannerV2
import com.rarible.blockchain.scanner.v2.event.NewBlockEvent
import com.rarible.blockchain.scanner.v2.event.RevertedBlockEvent
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class BlockScannerV2Test {


    private val mapper = TestBlockMapper()
    private val client = mockk<BlockchainBlockClient<TestBlockchainBlock>>()
    private val service = mockk<BlockService<TestBlock>>()
    private val scanner = BlockScannerV2(mapper, client, service, ScanRetryPolicyProperties())

    @BeforeEach
    fun beforeEach() {
        clearMocks(client, service)
    }

    @Test
    fun `watches events when state is empty`() = runBlocking {
        val block0 = TestBlockchainBlock(randomOriginalBlock("block0", 0, null))
        val block1 = TestBlockchainBlock(randomOriginalBlock("block1", 1, "block0"))

        every { client.newBlocks } returns flowOf(block1)
        coEvery { client.getBlock("block1") } returns block0
        coEvery { client.getBlock(0) } returns block0
        coEvery { client.getBlock(1) } returns block1

        coEvery { service.getBlock(any()) } returns null
        coEvery { service.getLastBlock() } returns null
        coEvery { service.save(any()) } answers { }

        val events = scanner.blockEvents.toList()
        assertThat(events).isEqualTo(
            listOf(
                NewBlockEvent(block0),
                NewBlockEvent(block1)
            )
        )

        coVerify(exactly = 1) { service.save(mapper.map(block0)) }
        coVerify(exactly = 1) { service.save(mapper.map(block1)) }
        coVerify(exactly = 1) { service.getLastBlock() }
        coVerify(exactly = 2) { client.getBlock(0) }
        coVerify(exactly = 1) { client.getBlock(1) }
        verify(exactly = 1) { client.newBlocks }
        confirmVerified(client, service)
    }

    @Test
    fun `watches simplest case when new blocks are added`() = runBlocking {
        val block0 = TestBlockchainBlock(randomOriginalBlock("block0", 0, null))
        val block1 = TestBlockchainBlock(randomOriginalBlock("block1", 1, "block0"))
        val block2 = TestBlockchainBlock(randomOriginalBlock("block2", 2, "block1"))

        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlock(0) } returns block0
        coEvery { client.getBlock(1) } returns block1
        coEvery { client.getBlock(2) } returns block2

        coEvery { service.getLastBlock() } returns mapper.map(block0)
        coEvery { service.save(any()) } answers { }

        val events = scanner.blockEvents.toList()
        assertThat(events).isEqualTo(
            listOf(
                NewBlockEvent(block1),
                NewBlockEvent(block2)
            )
        )

        coVerify(exactly = 1) { client.getBlock(0) }
        coVerify(exactly = 1) { client.getBlock(1) }
        coVerify(exactly = 1) { client.getBlock(2) }
        verify { client.newBlocks }
        coVerify(exactly = 1) { service.save(mapper.map(block1)) }
        coVerify(exactly = 1) { service.save(mapper.map(block2)) }
        coVerify(exactly = 1) { service.getLastBlock() }
        confirmVerified(client, service)
    }

    @Test
    fun `reorg chain works`() = runBlocking {
        val block0 = TestBlockchainBlock(randomOriginalBlock("block0", 0, null))
        val block1Reorg = TestBlockchainBlock(randomOriginalBlock("block1-reorg", 1, "block0"))
        val block2Reorg = TestBlockchainBlock(randomOriginalBlock("block2-reorg", 2, "block1-reorg"))
        val block1 = TestBlockchainBlock(randomOriginalBlock("block1", 1, "block0"))
        val block2 = TestBlockchainBlock(randomOriginalBlock("block2", 2, "block1"))
        val block3 = TestBlockchainBlock(randomOriginalBlock("block3", 3, "block2"))

        every { client.newBlocks } returns flowOf(block3)
        coEvery { client.getBlock("block0") } returns block0 //todo check if needed
        coEvery { client.getBlock("block1") } returns block1
        coEvery { client.getBlock("block2") } returns block2
        coEvery { client.getBlock(1) } returns block1
        coEvery { client.getBlock(2) } returns block2
        coEvery { client.getBlock(3) } returns block3

        coEvery { service.getLastBlock() } returns mapper.map(block2Reorg)
        coEvery { service.getBlock(0) } returns mapper.map(block0)
        coEvery { service.getBlock(1) } returns mapper.map(block1Reorg)
        coEvery { service.save(any()) } answers { }
        coEvery { service.remove(any()) } answers {}

        val events = scanner.blockEvents.toList()
        assertThat(events).isEqualTo(
            listOf(
                RevertedBlockEvent(block2Reorg),
                RevertedBlockEvent(block1Reorg),
                NewBlockEvent(block1),
                NewBlockEvent(block2),
                NewBlockEvent(block3)
            )
        )


        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlock("block0") }
        coVerify(exactly = 1) { client.getBlock("block1") }
        coVerify(exactly = 1) { client.getBlock(2) }
        coVerify(exactly = 1) { client.getBlock(3) }

        coVerify(exactly = 1) { service.getLastBlock() }
        coVerify(exactly = 1) { service.getBlock(0) }
        coVerify(exactly = 1) { service.getBlock(1) }
        coVerify(exactly = 1) { service.save(mapper.map(block1)) }
        coVerify(exactly = 1) { service.save(mapper.map(block2)) }
        coVerify(exactly = 1) { service.save(mapper.map(block3)) }
        coVerify(exactly = 1) { service.remove(block1Reorg.number) }
        coVerify(exactly = 1) { service.remove(block2Reorg.number) }
        confirmVerified(client, service)
    }

    @Test
    fun `reorg happens after start`() = runBlocking {
        val block0 = TestBlockchainBlock(randomOriginalBlock("block0", 0, null))
        val block1 = TestBlockchainBlock(randomOriginalBlock("block1", 1, "block0"))
        val block1New = TestBlockchainBlock(randomOriginalBlock("block1-new", 1, "block0"))
        val block2 = TestBlockchainBlock(randomOriginalBlock("block2", 2, "block1-new"))

        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlock(1) } returns block1
        coEvery { client.getBlock("block1-new") } returns block1New
        coEvery { client.getBlock(2) } returns block2
        coEvery { client.getBlock("block0") } returns block0

        coEvery { service.getLastBlock() } returns mapper.map(block1)
        coEvery { service.save(any()) } answers { }
        coEvery { service.getBlock(0) } returns mapper.map(block0)
        coEvery { service.remove(any()) } answers { }

        val events = scanner.blockEvents.toList()
        assertThat(events).isEqualTo(
            listOf(
                RevertedBlockEvent(block1),
                NewBlockEvent(block1New),
                NewBlockEvent(block2)
            )
        )

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlock(1) }
        coVerify(exactly = 1) { client.getBlock("block1-new") }
        coVerify(exactly = 1) { client.getBlock("block0") }
        coVerify(exactly = 1) { client.getBlock(2) }

        coVerify(exactly = 1) { service.getLastBlock() }
        coVerify(exactly = 1) { service.save(mapper.map(block2)) }
        coVerify(exactly = 1) { service.save(mapper.map(block1New)) }
        coVerify(exactly = 1) { service.getBlock(0) }
        coVerify(exactly = 1) { service.remove(block1.number) }
        confirmVerified(client, service)
    }
}
