package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationIndexer<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val logEventHandler: LogEventHandler<OB, OL, L>,
    private val batchSize: Long
) {

    private val logger: Logger = LoggerFactory.getLogger(logEventHandler.subscriber.javaClass)

    fun reindex(from: Long, to: Long): Flow<LongRange> {
        logger.info("Scanning for Logs in batches from={} to={} with batchSize={}", from, to, batchSize)
        val ranges = BlockRanges.getRanges(from, to, batchSize)
        val rangeFlow = ranges.map { range ->
            val descriptor = logEventHandler.subscriber.getDescriptor()
            val blocks = blockchainClient.getBlockEvents(descriptor, range)
            blocks.collect {
                reindexBlock(it)
            }
            range
        }
        return rangeFlow
    }

    private suspend fun reindexBlock(logs: BlockLogs<OL>): Flow<L> {
        logger.info("Reindexing Block {} with {} Logs", logs.blockHash, logs.logs.size)
        val block = blockchainClient.getBlock(logs.blockHash)
        return logEventHandler.handleLogs(block, logs.logs)
    }

}