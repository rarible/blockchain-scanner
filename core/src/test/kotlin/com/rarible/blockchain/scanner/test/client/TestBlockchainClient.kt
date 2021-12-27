package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

class TestBlockchainClient(
    private val data: TestBlockchainData
) : BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor> {

    private val blocksByNumber = data.blocks.associateBy { it.number }
    private val logsByBlock = data.logs.filter { it.blockHash != null }.groupBy { it.blockHash }

    override val newBlocks: Flow<TestBlockchainBlock> get() = data.newBlocks.asFlow()

    override suspend fun getBlock(number: Long): TestBlockchainBlock = blocksByNumber.getValue(number)

    override fun getBlockLogs(
        descriptor: TestDescriptor,
        range: LongRange
    ): Flow<FullBlock<TestBlockchainBlock, TestBlockchainLog>> = flatten {
        blocksByNumber.values.filter { range.contains(it.number) }
            .sortedBy { it.number }
            .map { FullBlock(it, getBlockLogs(descriptor, it)) }
            .asFlow()
    }

    private fun getBlockLogs(
        descriptor: TestDescriptor,
        block: TestBlockchainBlock
    ): List<TestBlockchainLog> =
        logsByBlock[block.hash]!!
            .filter { it.topic == descriptor.id }
            .mapIndexed { index, log -> TestBlockchainLog(log, index) }
}
