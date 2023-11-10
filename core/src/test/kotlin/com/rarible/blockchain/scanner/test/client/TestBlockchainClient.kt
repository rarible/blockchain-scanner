package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

class TestBlockchainClient(
    private val data: TestBlockchainData
) : BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor> {

    private val blocksByNumber = data.blocks.associateBy { it.number }
    private val logsByBlockHash = data.logs.filter { it.blockHash != null }.groupBy { it.blockHash }

    override val newBlocks: Flow<TestBlockchainBlock> get() = data.newBlocks.asFlow()

    override suspend fun getBlocks(numbers: List<Long>): List<TestBlockchainBlock> = numbers.mapNotNull { getBlock(it) }

    override suspend fun getBlock(number: Long): TestBlockchainBlock? = blocksByNumber[number]

    override fun getBlockLogs(
        descriptor: TestDescriptor,
        blocks: List<TestBlockchainBlock>,
        stable: Boolean,
        mode: ScanMode
    ): Flow<FullBlock<TestBlockchainBlock, TestBlockchainLog>> {
        val numbers = blocks.map { it.number }.toSet()
        return blocksByNumber.values.filter { it.number in numbers }
            .sortedBy { it.number }
            .map { FullBlock(it, getBlockLogs(descriptor, it)) }
            .asFlow()
    }

    private fun getBlockLogs(
        descriptor: TestDescriptor,
        block: TestBlockchainBlock
    ): List<TestBlockchainLog> =
        (logsByBlockHash[block.hash] ?: emptyList())
            .filter { it.topic == descriptor.id }
            .mapIndexed { index, log -> TestBlockchainLog(log, index) }

    override suspend fun getFirstAvailableBlock(): TestBlockchainBlock = getBlock(0)!!

    override suspend fun getLastBlockNumber(): Long {
        return data.blocks.maxOf { it.number }
    }
}
