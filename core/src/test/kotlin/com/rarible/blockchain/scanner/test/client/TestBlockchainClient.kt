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
    data: TestBlockchainData
) : BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor> {

    private val blocksByNumber = data.blocks.associateBy { it.number }
    private val blocksByHash = data.blocks.associateBy { it.hash }
    private val logsByBlock = data.logs.filter { it.blockHash != null }.groupBy { it.blockHash }

    override val newBlocks: Flow<TestBlockchainBlock> =
        data.newBlocks.asFlow().map { TestBlockchainBlock(it) }

    override suspend fun getBlock(number: Long): TestBlockchainBlock {
        return TestBlockchainBlock(blocksByNumber[number]!!)
    }

    override suspend fun getBlock(hash: String): TestBlockchainBlock {
        return TestBlockchainBlock(blocksByHash[hash]!!)
    }

    override fun getBlockLogs(
        descriptor: TestDescriptor,
        range: LongRange
    ): Flow<FullBlock<TestBlockchainBlock, TestBlockchainLog>> = flatten {
        blocksByNumber.values.filter { range.contains(it.number) }
            .sortedBy { it.number }
            .map { FullBlock(TestBlockchainBlock(it), getBlockLogs(descriptor, TestBlockchainBlock(it))) }
            .asFlow()
    }

    private fun getBlockLogs(
        descriptor: TestDescriptor,
        block: TestBlockchainBlock
    ): List<TestBlockchainLog> =
        logsByBlock[block.hash]!!.filter { it.topic == descriptor.id }
            .mapIndexed { index, log -> TestBlockchainLog(log, index) }
}
