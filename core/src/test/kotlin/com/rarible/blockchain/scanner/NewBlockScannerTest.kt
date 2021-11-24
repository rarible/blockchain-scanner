package com.rarible.blockchain.scanner

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class NewBlockScannerTest {

    @Test
    fun `watches events when state is empty`() = runBlocking {
        val block0 = Block(0, "block0", null)
        val block1 = Block(1, "block1", "block0")
        val client = mockk<BlockClient>()
        every { client.newBlocks } returns flowOf(block1)
        coEvery { client.getBlockByHash("block1") } returns block0
        coEvery { client.getBlockByNumber(0) } returns block0
        coEvery { client.getBlockByNumber(1) } returns block1

        val service = mockk<BlockService>()
        coEvery { service.getBlockByNumber(any()) } returns null
        coEvery { service.getLastKnownBlock() } returns null
        coEvery { service.saveBlock(any()) } answers { }
        val scanner = NewBlockScanner(client, service)

        scanner.blockEvents.toList()

        coVerify(exactly = 1) { service.saveBlock(block0) }
        coVerify(exactly = 1) { service.saveBlock(block1) }
        coVerify(exactly = 1) { service.getLastKnownBlock() }
        coVerify(exactly = 2) { client.getBlockByNumber(0) }
        coVerify(exactly = 1) { client.getBlockByNumber(1) }
        verify(exactly = 1) { client.newBlocks }
        confirmVerified(client, service)
    }

    @Test
    fun `watches simplest case when new blocks are added`() = runBlocking {
        val block0 = Block(0, "block0", null)
        val block1 = Block(1, "block1", "block0")
        val block2 = Block(2, "block2", "block1")

        val client = mockk<BlockClient>()
        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlockByNumber(0) } returns block0
        coEvery { client.getBlockByNumber(1) } returns block1
        coEvery { client.getBlockByNumber(2) } returns block2

        val storage = mockk<BlockService>()
        coEvery { storage.getLastKnownBlock() } returns block0
        coEvery { storage.saveBlock(any()) } answers { }

        val scanner = NewBlockScanner(client, storage)
        scanner.blockEvents.toList()

        coVerify(exactly = 1) { client.getBlockByNumber(0) }
        coVerify(exactly = 1) { client.getBlockByNumber(1) }
        coVerify(exactly = 1) { client.getBlockByNumber(2) }
        verify { client.newBlocks }
        coVerify(exactly = 1) { storage.saveBlock(block1) }
        coVerify(exactly = 1) { storage.saveBlock(block2) }
        coVerify(exactly = 1) { storage.getLastKnownBlock() }
        confirmVerified(client, storage)
    }

    @Test
    fun `reorg chain works`() = runBlocking {
        val block0 = Block(0, "block0", null)
        val block1Reorg = Block(1, "block1-reorg", "block0")
        val block1 = Block(1, "block1", "block0")
        val block2 = Block(2, "block2", "block1")

        val client = mockk<BlockClient>()
        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlockByHash("block0") } returns block0
        coEvery { client.getBlockByNumber(1) } returns block1
        coEvery { client.getBlockByNumber(2) } returns block2

        val storage = mockk<BlockService>()
        coEvery { storage.getLastKnownBlock() } returns block1Reorg
        coEvery { storage.getBlockByNumber(0) } returns block0
        coEvery { storage.saveBlock(any()) } answers { }

        val scanner = NewBlockScanner(client, storage)
        scanner.blockEvents.toList()

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlockByHash("block0") }
        coVerify(exactly = 1) { client.getBlockByNumber(1) }
        coVerify(exactly = 1) { client.getBlockByNumber(2) }
        coVerify(exactly = 1) { storage.getLastKnownBlock() }
        coVerify(exactly = 1) { storage.getBlockByNumber(0) }
        coVerify(exactly = 1) { storage.saveBlock(block1) }
        coVerify(exactly = 1) { storage.saveBlock(block2) }
        confirmVerified(client, storage)
    }

    @Test
    fun `reorg happens after start`() = runBlocking {
        val block0 = Block(0, "block0", null)
        val block1 = Block(1, "block1", "block0")
        val block1New = Block(1, "block1-new", "block0")
        val block2 = Block(2, "block2", "block1-new")

        val client = mockk<BlockClient>()
        every { client.newBlocks } returns flowOf(block2)
        coEvery { client.getBlockByNumber(1) } returns block1
        coEvery { client.getBlockByHash("block1-new") } returns block1New
        coEvery { client.getBlockByNumber(2) } returns block2
        coEvery { client.getBlockByHash("block0") } returns block0

        val storage = mockk<BlockService>()
        coEvery { storage.getLastKnownBlock() } returns block1
        coEvery { storage.saveBlock(any()) } answers { }
        coEvery { storage.getBlockByNumber(0) } returns block0

        val scanner = NewBlockScanner(client, storage)
        scanner.blockEvents.toList()

        verify(exactly = 1) { client.newBlocks }
        coVerify(exactly = 1) { client.getBlockByNumber(1) }
        coVerify(exactly = 1) { client.getBlockByHash("block1-new") }
        coVerify(exactly = 1) { client.getBlockByHash("block0") }
        coVerify(exactly = 1) { client.getBlockByNumber(2) }

        coVerify(exactly = 1) { storage.getLastKnownBlock() }
        coVerify(exactly = 1) { storage.saveBlock(block2) }
        coVerify(exactly = 1) { storage.saveBlock(block1New) }
        coVerify(exactly = 1) { storage.getBlockByNumber(0) }
        confirmVerified(client, storage)
    }
}

