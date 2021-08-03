package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.job.PendingLogsCheckJob
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
class DefaultPendingLogChecker<BB : BlockchainBlock, BL : BlockchainLog, L : Log, D : LogEventDescriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val blockListener: BlockListener,
    private val descriptors: List<D>,
    private val logService: LogService<L, D>,
    private val logEventListeners: List<LogEventListener<L>>
) : PendingLogChecker {

    override fun checkPendingLogs() {
        runBlocking {
            val collections = descriptors.asFlow().flatMapConcat { descriptors ->
                logService.findPendingLogs(descriptors)
                    .mapNotNull { processLog(descriptors, it) }
            }.toCollection(mutableListOf())


            val droppedLogs = collections.mapNotNull { it.first }
            val newBlocks = collections.mapNotNull { it.second }.distinctBy { it.hash }

            onDroppedLogs(droppedLogs)
            onNewBlocks(newBlocks)
        }
    }

    private suspend fun onDroppedLogs(droppedLogs: List<L>) {
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

    private suspend fun processLog(descriptor: D, log: L): Pair<L?, BlockchainBlock?>? {
        val tx = blockchainClient.getTransactionMeta(log.transactionHash)

        if (tx == null) {
            logger.info("for log $log\nnot found transaction. dropping it")
            val updatedLog = markLogAsDropped(log, descriptor)
            return Pair(updatedLog, null)
        } else {
            val blockHash = tx.blockHash
            if (blockHash == null) {
                logger.info("for log $log\nfound transaction $tx\nit's pending. skip it")
                return null
            }
            logger.info("for log $log\nfound transaction $tx\nit's confirmed. update logs for its block")
            val block = blockchainClient.getBlock(blockHash)
            return Pair(null, block)
        }
    }

    private suspend fun markLogAsDropped(log: L, descriptor: D): L {
        return logService.updateStatus(descriptor, log, Log.Status.DROPPED)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingLogsCheckJob::class.java)
    }
}