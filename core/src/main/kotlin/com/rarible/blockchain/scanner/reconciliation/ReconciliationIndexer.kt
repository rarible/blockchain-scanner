package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.LogEventPublisher
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory

/**
 * Reconciliation indexer for single LogEventHandler (which means, for single subscriber).
 * The goal of such indexer to re-read LogEvents from blocks, update them and publish changes.
 * Indexer do NOT check pending blocks/logs and also do NOT update state of blocks.
 */
@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationIndexer<BB : BlockchainBlock, B: Block, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logEventHandler: LogEventHandler<BB, BL, L, R, D>,
    private val logEventPublisher: LogEventPublisher<L, R>,
    private val blockService: BlockService<B>,
    private val blockMapper: BlockMapper<BB, B>
) {

    private val logger = LoggerFactory.getLogger(logEventHandler.subscriber.javaClass)

    fun reindex(from: Long, to: Long, batchSize: Long): Flow<LongRange> {
        logger.info("Scanning for Logs in batches from={} to={} with batchSize={}", from, to, batchSize)
        return BlockRanges.getRanges(from, to, batchSize).onEach { range ->
            val descriptor = logEventHandler.subscriber.getDescriptor()
            blockchainClient.getBlockEvents(descriptor, range).onEach {
                val processedLogs = reindexBlock(it)
                val blockEvent = BlockEvent(Source.REINDEX, it.block)
                logEventPublisher.onBlockProcessed(blockEvent, processedLogs)
            }
        }
    }

    private suspend fun reindexBlock(fullBlock: FullBlock<BB, BL>): List<R> {
        logger.info("Reindexing Block {} with {} Logs", fullBlock.block.hash, fullBlock.logs.size)
        return logEventHandler.handleLogs(fullBlock).onCompletion {
            blockService.save(blockMapper.map(fullBlock.block, Block.Status.SUCCESS))
        }.toList()
    }

}
