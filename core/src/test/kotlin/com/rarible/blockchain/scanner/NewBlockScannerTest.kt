package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.v2.BlockScannerV2
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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class NewBlockScannerTest {

    private val mapper = TestBlockMapper()
    private val client = mockk<BlockchainBlockClient<TestBlockchainBlock>>()
    private val service = mockk<BlockService<TestBlock>>()
    private val scanner = BlockScannerV2(mapper, client, service)

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

        scanner.blockEvents.toList()

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

        scanner.blockEvents.toList()

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
        val block1 = TestBlockchainBlock(randomOriginalBlock("block1", 1, "block0"))
        val block2 = TestBlockchainBlock(randomOriginalBlock("block2", 2, "block1"))

        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlock("block0") } returns block0
        coEvery { client.getBlock(1) } returns block1
        coEvery { client.getBlock(2) } returns block2

        coEvery { service.getLastBlock() } returns mapper.map(block1Reorg)
        coEvery { service.getBlock(0) } returns mapper.map(block0)
        coEvery { service.save(any()) } answers { }

        scanner.blockEvents.toList()

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlock("block0") }
        coVerify(exactly = 1) { client.getBlock(1) }
        coVerify(exactly = 1) { client.getBlock(2) }
        coVerify(exactly = 1) { service.getLastBlock() }
        coVerify(exactly = 1) { service.getBlock(0) }
        coVerify(exactly = 1) { service.save(mapper.map(block1)) }
        coVerify(exactly = 1) { service.save(mapper.map(block2)) }
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

        scanner.blockEvents.toList()

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlock(1) }
        coVerify(exactly = 1) { client.getBlock("block1-new") }
        coVerify(exactly = 1) { client.getBlock("block0") }
        coVerify(exactly = 1) { client.getBlock(2) }

        coVerify(exactly = 1) { service.getLastBlock() }
        coVerify(exactly = 1) { service.save(mapper.map(block2)) }
        coVerify(exactly = 1) { service.save(mapper.map(block1New)) }
        coVerify(exactly = 1) { service.getBlock(0) }
        confirmVerified(client, service)
    }
}

