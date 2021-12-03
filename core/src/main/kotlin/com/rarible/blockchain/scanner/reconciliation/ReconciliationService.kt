package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.event.log.LogEventHandler
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow

/**
 * Reconciliation service contains set of single Reconciliation Indexers(one per subscriber) and
 * triggers reconciliation procedure for any of them from specified block until last known.
 */
@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationService<BB : BlockchainBlock, B : Block, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    logEventPublisher: LogEventPublisher,
    private val blockService: BlockService<B>,
    blockMapper: BlockMapper<BB, B>
) {

    // Making single LogEventHandler for each subscriber
    private val indexers = subscribers.map {
        LogEventHandler(it, logMapper, logService)
    }.associate {
        it.subscriber.getDescriptor().id to createIndexer(it, logEventPublisher, blockService, blockMapper)
    }

    fun reindex(descriptorId: String?, from: Long, batchSize: Long): Flow<LongRange> = flatten {
        val lastBlockNumber = blockService.getLastBlock()?.id ?: 0
        reindex(descriptorId, from, lastBlockNumber, batchSize)
    }

    private fun reindex(descriptorId: String?, from: Long, to: Long, batchSize: Long): Flow<LongRange> {
        val blockIndexer = indexers[descriptorId]
            ?: throw IllegalArgumentException(
                "BlockIndexer for descriptor '$descriptorId' not found," +
                        " available descriptors: ${indexers.keys}"
            )

        return blockIndexer.reindex(from, to, batchSize)
    }

    private fun createIndexer(
        logEventHandler: LogEventHandler<BB, BL, L, R, D>,
        logEventPublisher: LogEventPublisher,
        blockService: BlockService<B>,
        blockMapper: BlockMapper<BB, B>
    ): ReconciliationIndexer<BB, B, BL, L, R, D> {
        return ReconciliationIndexer(
            blockchainClient = blockchainClient,
            logEventHandler = logEventHandler,
            logEventPublisher = logEventPublisher,
            blockService = blockService,
            blockMapper = blockMapper
        )
    }
}
