package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
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
class DefaultPendingLogChecker<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logService: LogService<L, R, D>,
    private val descriptors: List<D>,
    private val blockListener: BlockListener,
    private val logEventListeners: List<LogEventListener<L, R>>
) : PendingLogChecker {

    private val logger = LoggerFactory.getLogger(DefaultPendingLogChecker::class.java)

    override suspend fun checkPendingLogs() {
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
                logger.error("Caught exception while onDroppedLogs logs of listener: {}", it.javaClass, ex)
            }
        }
    }

    private suspend fun onNewBlocks(newBlocks: List<BlockchainBlock>) {
        blockListener.onBlockEvents(newBlocks.map {
            NewBlockEvent(Source.PENDING, it.number, it.hash)
        })
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
                logger.info("Found pending transaction [{}] for log [{}], skipping", tx, record)
                return null
            }
            val block = blockchainClient.getBlock(blockHash)
            logger.info(
                "Found confirmed transaction [{}] for log [{}], updating entire block [{}:{}]",
                tx, record, block.number, blockHash
            )
            return Pair(null, block)
        }
    }

    private suspend fun markLogAsDropped(record: R, descriptor: D): R {
        return logService.updateStatus(descriptor, record, Log.Status.DROPPED)
    }
}
