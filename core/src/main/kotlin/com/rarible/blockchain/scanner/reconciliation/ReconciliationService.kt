package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.apm.withTransaction
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import org.slf4j.LoggerFactory

/**
 * Reconciliation service contains set of single Reconciliation Indexers(one per subscriber) and
 * triggers reconciliation procedure for any of them from specified block until last known.
 */
class ReconciliationService<BB : BlockchainBlock, B : Block, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    private val blockService: BlockService<B>,
    private val blockEventListeners: Map<String, BlockEventListener<BB, BL, L, R, D>>
) {

    private val logger = LoggerFactory.getLogger(ReconciliationService::class.java)

    fun reindex(subscriberGroupId: String?, from: Long, batchSize: Long): Flow<LongRange> = flatten {
        val lastBlockNumber = blockService.getLastBlock()?.id ?: 0
        reindex(subscriberGroupId, from, lastBlockNumber, batchSize)
    }

    private fun reindex(subscriberGroupId: String?, from: Long, to: Long, batchSize: Long): Flow<LongRange> {
        val listener = blockEventListeners[subscriberGroupId]
            ?: throw IllegalArgumentException(
                "BlockIndexer for subscriber group '$subscriberGroupId' not found," +
                        " available descriptors: ${blockEventListeners.keys}"
            )

        return reindex(listener, from, to, batchSize)
    }

    private fun reindex(blockListener: BlockListener, from: Long, to: Long, batchSize: Long): Flow<LongRange> {
        logger.info("Scanning for Logs in batches from {} to {} with batchSize {}", from, to, batchSize)
        return BlockRanges.getRanges(from, to, batchSize.toInt()).onEach { range ->
            withTransaction("reindex", listOf("range" to range.toString())) {
                val reindexBlockEvents = range.map { ReindexBlockEvent(it) }
                blockListener.onBlockEvents(reindexBlockEvents)
            }
        }
    }
}
