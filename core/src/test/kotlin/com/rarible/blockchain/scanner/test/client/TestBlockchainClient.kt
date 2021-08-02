package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import java.util.*

class TestBlockchainClient(
    data: TestBlockchainData
) : BlockchainClient<TestBlockchainBlock, TestBlockchainLog> {

    val blocksByNumber = data.blocks.associateBy { it.number }
    val blocksByHash = data.blocks.associateBy { it.hash }
    val lastBlock = data.blocks.maxBy { it.number }
    val logs = data.logs.groupBy { it.blockHash }

    override fun listenNewBlocks(): Flow<TestBlockchainBlock> {
        TODO("Not yet implemented")
    }

    override suspend fun getBlock(id: Long): TestBlockchainBlock {
        return TestBlockchainBlock(blocksByNumber[id]!!)
    }

    override suspend fun getBlock(hash: String): TestBlockchainBlock {
        return TestBlockchainBlock(blocksByHash[hash]!!)
    }

    override suspend fun getLastBlockNumber(): Long {
        return lastBlock!!.number
    }

    override suspend fun getBlockEvents(
        block: TestBlockchainBlock,
        descriptor: LogEventDescriptor
    ): List<TestBlockchainLog> {
        return logs[block.hash]!!.filter { it.topic.equals(descriptor.topic) }.map { TestBlockchainLog(it) }
    }

    override fun getBlockEvents(descriptor: LogEventDescriptor, range: LongRange): Flow<BlockLogs<TestBlockchainLog>> {
        return blocksByNumber.values.filter { range.contains(it.number) }
            .sortedBy { it.number }
            .asFlow()
            .map { BlockLogs(it.hash, getBlockEvents(TestBlockchainBlock(it), descriptor)) }
    }

    override suspend fun getTransactionMeta(transactionHash: String): Optional<TransactionMeta> {
        TODO("Not yet implemented")
    }
}