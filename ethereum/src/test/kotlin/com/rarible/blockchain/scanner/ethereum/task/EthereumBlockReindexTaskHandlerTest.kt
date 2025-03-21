package com.rarible.blockchain.scanner.ethereum.task

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumClientFactory
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.reindex.BlockRange
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class EthereumBlockReindexTaskHandlerTest {

    private fun setupTestEnvironment(): Triple<EthereumScannerManager, BlockService, ReindexMonitor> {
        val client: EthereumBlockchainClient = mockk()
        val blockService: BlockService = mockk()
        val reindexMonitor: ReindexMonitor = mockk()
        val manager = EthereumScannerManager(
            client,
            emptyList(),
            blockService,
            mockk(),
            EthereumScannerProperties(),
            mockk(),
            mockk(),
            mockk(),
            reindexMonitor,
            emptyList(),
            mockk(),
            mockk()
        )
        val block = Block(
            id = 1,
            hash = "0x",
            timestamp = 0,
            parentHash = "0x",
            status = BlockStatus.SUCCESS
        )
        coEvery { blockService.getBlock(any()) } returns block
        coEvery { client.getBlock(any()) } returns null
        coEvery { client.getLastBlockNumber() } returns 42
        coEvery { client.getBlocks(any()) } returns emptyList()

        return Triple(manager, blockService, reindexMonitor)
    }

    @Test
    fun `reindex task has last block`() = runBlocking {
        val (manager, blockService, reindexMonitor) = setupTestEnvironment()
        coEvery { reindexMonitor.onReindex(0, 9, any(), any()) } returns Unit

        val reindexClient = mockk<EthereumClient> {
            coEvery { getLastBlockNumber() } returns 42
            coEvery { getBlocks(any()) } returns emptyList()
        }

        val blockchainClientFactory: EthereumClientFactory = mockk()
        every { blockchainClientFactory.createReindexClient() } returns reindexClient
        every { blockchainClientFactory.createReconciliationClient() } returns reindexClient
        val task = EthereumBlockReindexTaskHandler(
            manager = manager,
            blockchainClientFactory = blockchainClientFactory,
        )
        val param = EthereumReindexParam(
            name = "reindex",
            range = BlockRange(0, 9, null),
            topics = emptyList(),
            addresses = emptyList()
        )
        task.runLongTask(null, param.toJson()).collect()

        coVerify(exactly = 0) { blockService.getLastBlock() }
        coVerify(exactly = 1) { reindexMonitor.onReindex(0, 9, 7.0, "reindex") }
    }

    @Test
    fun `reindex task doesn't have last block`() = runBlocking {
        val (manager, blockService, reindexMonitor) = setupTestEnvironment()
        coEvery { reindexMonitor.onReindex(0, 42, any(), any()) } returns Unit
        val block = Block(
            id = 42,
            hash = "0x",
            timestamp = 0,
            parentHash = "0x",
            status = BlockStatus.SUCCESS
        )
        coEvery { blockService.getLastBlock() } returns block

        val reindexClient = mockk<EthereumClient> {
            coEvery { getLastBlockNumber() } returns 42
            coEvery { getBlocks(any()) } returns emptyList()
        }

        val blockchainClientFactory: EthereumClientFactory = mockk()
        every { blockchainClientFactory.createReindexClient() } returns reindexClient
        every { blockchainClientFactory.createReconciliationClient() } returns mockk()
        val task = EthereumBlockReindexTaskHandler(
            manager = manager,
            blockchainClientFactory = blockchainClientFactory,
        )
        val param = EthereumReindexParam(
            name = "reindex",
            range = BlockRange(0, null, null),
            topics = emptyList(),
            addresses = emptyList()
        )
        task.runLongTask(null, param.toJson()).collect()

        coVerify(exactly = 1) { blockService.getLastBlock() }
        coVerify(exactly = 1) { reindexMonitor.onReindex(0, 42, 8.0, "reindex") }
    }
}
