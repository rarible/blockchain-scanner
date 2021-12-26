package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.apm.withTransaction
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import org.slf4j.LoggerFactory

class ReconciliationService<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val blockService: BlockService,
    private val blockEventListeners: Map<String, BlockEventListener<BB, BL, R, D>>
) {

    private val logger = LoggerFactory.getLogger(ReconciliationService::class.java)

    fun reindex(groupId: String, from: Long, batchSize: Long): Flow<LongRange> = flatten {
        val lastBlockNumber = blockService.getLastBlock()?.id ?: 0
        val listener = blockEventListeners[groupId]
            ?: throw IllegalArgumentException(
                "BlockIndexer for subscriber group '$groupId' not found," +
                    " available descriptors: ${blockEventListeners.keys}"
            )
        logger.info(
            "Scanning for Logs in batches from {} to {} with batchSize {} for group {}",
            from,
            lastBlockNumber,
            batchSize,
            groupId
        )
        BlockRanges.getRanges(from, lastBlockNumber, batchSize.toInt()).onEach { range ->
            withTransaction("reindex", listOf("range" to range.toString())) {
                val reindexBlockEvents = range.map { ReindexBlockEvent(it) }
                listener.onBlockEvents(reindexBlockEvents)
            }
        }
    }
}
