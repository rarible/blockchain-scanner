package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.logging.LoggingUtils
import com.rarible.core.logging.loggerContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toMono

class ReconciliationIndexer<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val logEventHandler: LogEventHandler<OB, OL, L>,
    private val batchSize: Long
) {

    private val logger: Logger = LoggerFactory.getLogger(logEventHandler.subscriber.javaClass)

    fun reindex(from: Long, to: Long): Flux<LongRange> {
        return LoggingUtils.withMarkerFlux { marker ->
            logger.info(marker, "loading logs in batches from=$from to=$to batchSize=$batchSize")
            val ranges = BlockRanges.getRanges(from, to, batchSize)
            ranges.concatMap { range ->
                val descriptor = logEventHandler.subscriber.getDescriptor()
                val blocks = blockchainClient.getBlockEvents(descriptor, range, marker)
                blocks.flatMap {
                    reindexBlock(it)
                }.toMono().thenReturn(range)
            }
        }
    }

    private fun reindexBlock(logs: BlockLogs<OL>): Flux<L> {
        return LoggingUtils.withMarkerFlux { marker ->
            logger.info(marker, "reindex. processing block ${logs.blockHash} logs: ${logs.logs.size}")
            blockchainClient.getBlock(logs.blockHash)
                .flatMapMany { block -> logEventHandler.handleLogs(marker, block, logs.logs) }
        }.loggerContext(mapOf("blockHash" to logs.blockHash))
    }

}