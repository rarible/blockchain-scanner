package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.LogEventPublisher
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toCollection
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationIndexer<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logEventHandler: LogEventHandler<BB, BL, L, R, D>,
    private val logEventPublisher: LogEventPublisher<L, R>,
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
                val processedLogs = reindexBlock(it)
                val blockEvent = BlockEvent(Source.REINDEX, it.block)
                logEventPublisher.onBlockProcessed(blockEvent, processedLogs)
            }
            range
        }
        return rangeFlow
    }

    private suspend fun reindexBlock(fullBlock: FullBlock<BB, BL>): List<R> {
        logger.info("Reindexing Block {} with {} Logs", fullBlock.block.hash, fullBlock.logs.size)
        return logEventHandler.handleLogs(fullBlock).toCollection(mutableListOf())
    }

}