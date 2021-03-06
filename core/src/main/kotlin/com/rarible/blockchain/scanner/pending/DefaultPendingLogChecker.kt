package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toCollection
import org.slf4j.LoggerFactory

@FlowPreview
class DefaultPendingLogChecker<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logService: LogService<L, R, D>,
    private val descriptors: List<D>,
    private val blockListener: BlockListener,
    private val logEventListeners: List<LogEventListener<L, R>>
) : PendingLogChecker {

    private val logger = LoggerFactory.getLogger(DefaultPendingLogChecker::class.java)

    suspend override fun checkPendingLogs() {
        val collections = descriptors.asFlow().flatMapConcat { descriptors ->
            logService.findPendingLogs(descriptors)
                .mapNotNull { processLog(descriptors, it) }
        }.toCollection(mutableListOf())


        val droppedLogs = collections.mapNotNull { it.first }
        val newBlocks = collections.mapNotNull { it.second }.distinctBy { it.hash }

        onDroppedLogs(droppedLogs)
        onNewBlocks(newBlocks)
    }

    private suspend fun onDroppedLogs(droppedLogs: List<R>) {
        logEventListeners.forEach {
            try {
                it.onPendingLogsDropped(droppedLogs)
            } catch (ex: Throwable) {
                logger.error("caught exception while onDroppedLogs logs of listener: {}", it.javaClass, ex)
            }
        }
    }

    private suspend fun onNewBlocks(newBlocks: List<BlockchainBlock>) {
        newBlocks.forEach {
            blockListener.onBlockEvent(BlockEvent(Source.PENDING, it))
        }
    }

    private suspend fun processLog(descriptor: D, record: R): Pair<R?, BlockchainBlock?>? {
        val tx = blockchainClient.getTransactionMeta(record.log!!.transactionHash)

        if (tx == null) {
            logger.info("Can't find transaction for record in blockchain, dropping it: [{}]", record)
            val updatedLog = markLogAsDropped(record, descriptor)
            return Pair(updatedLog, null)
        } else {
            val blockHash = tx.blockHash
            if (blockHash == null) {
                logger.info("for log $record\nfound transaction $tx\nit's pending. skip it")
                return null
            }
            logger.info("for log $record\nfound transaction $tx\nit's confirmed. update logs for its block")
            val block = blockchainClient.getBlock(blockHash)
            return Pair(null, block)
        }
    }

    private suspend fun markLogAsDropped(record: R, descriptor: D): R {
        return logService.updateStatus(descriptor, record, Log.Status.DROPPED)
    }
}
