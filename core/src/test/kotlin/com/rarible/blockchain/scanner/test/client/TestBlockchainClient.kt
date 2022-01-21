package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

class TestBlockchainClient(
    data: TestBlockchainData
) : BlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor> {

    private val blocksByNumber = data.blocks.associateBy { it.number }
    private val blocksByHash = data.blocks.associateBy { it.hash }
    private val lastBlock = data.blocks.maxByOrNull { it.number }
    private val logs = data.logs
    private val logsByBlock = data.logs.filter { it.blockHash != null }.groupBy { it.blockHash }
    private val newBlocks = data.newBlocks

    override fun listenNewBlocks(): Flow<TestBlockchainBlock> {
        return newBlocks.asFlow().map { TestBlockchainBlock(it) }
    }

    override suspend fun getBlock(number: Long): TestBlockchainBlock {
        return TestBlockchainBlock(blocksByNumber[number]!!)
    }

    override suspend fun getBlock(hash: String): TestBlockchainBlock {
        return TestBlockchainBlock(blocksByHash[hash]!!)
    }

    override suspend fun getLastBlockNumber(): Long {
        return lastBlock!!.number
    }

    override fun getBlockEvents(
        descriptor: TestDescriptor,
        block: TestBlockchainBlock
    ): Flow<TestBlockchainLog> {
        return logsByBlock[block.hash]!!.filter { it.topic == descriptor.topic }.map { TestBlockchainLog(it) }.asFlow()
    }

    override fun getBlockEvents(
        descriptor: TestDescriptor,
        range: LongRange
    ): Flow<FullBlock<TestBlockchainBlock, TestBlockchainLog>> = flow {
        emitAll(
            blocksByNumber.values.filter { range.contains(it.number) }
                .sortedBy { it.number }
                .map { FullBlock(TestBlockchainBlock(it), getBlockEvents(descriptor, TestBlockchainBlock(it)).toList()) }.asFlow()
        )
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val log = logs.find { it.transactionHash == transactionHash }
        return if (log == null) null else TransactionMeta(log.transactionHash, log.blockHash)
    }
}
