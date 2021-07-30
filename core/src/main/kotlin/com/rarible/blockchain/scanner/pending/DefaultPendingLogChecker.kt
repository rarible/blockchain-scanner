package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.job.PendingLogsCheckJob
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DefaultPendingLogChecker<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val blockListener: BlockListener,
    private val collections: Set<String>,
    private val logService: LogService<L>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>
) : PendingLogChecker {

    override fun checkPendingLogs() {
        runBlocking {
            val collections = collections.asFlow().flatMapConcat { collection ->
                logService.findPendingLogs(collection)
                    .mapNotNull { processLog(collection, it) }
            }.toCollection(mutableListOf())


            val droppedLogs = collections.mapNotNull { it.first }
            val newBlocks = collections.mapNotNull { it.second }.distinctBy { it.hash }

            onDroppedLogs(droppedLogs)
            onNewBlocks(newBlocks)
        }
    }

    private suspend fun onDroppedLogs(droppedLogs: List<L>) {
        logEventPostProcessors.forEach {
            try {
                it.postProcessLogs(droppedLogs)
            } catch (ex: Throwable) {
                logger.error("caught exception while onDroppedLogs logs of listener: {}", it.javaClass, ex)
            }
        }
        // TODO ???
        /*return (logEventPostProcessors ?: emptyList()).asFlow()
            .map { logEventsListener ->
                logEventsListener.postProcessLogs(droppedLogs).onErrorResume { th ->
                    logger.error(
                        "caught exception while onDroppedLogs logs of listener: {}",
                        logEventsListener.javaClass, th
                    )
                    Mono.empty()
                }
            }.then()
            */
    }

    private suspend fun onNewBlocks(newBlocks: List<BlockchainBlock>) {
        newBlocks.forEach {
            blockListener.onBlockEvent(BlockEvent(it))
        }
    }

    private suspend fun processLog(collection: String, log: L): Pair<L?, BlockchainBlock?>? {
        val txOption = blockchainClient.getTransactionMeta(log.transactionHash)

        if (!txOption.isPresent) {
            logger.info("for log $log\nnot found transaction. dropping it")
            val updatedLog = markLogAsDropped(log, collection)
            return Pair(updatedLog, null)
        } else {
            val tx = txOption.get()
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

    private suspend fun markLogAsDropped(log: L, collection: String): L {
        return logService.updateStatus(collection, log, Log.Status.DROPPED)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingLogsCheckJob::class.java)
    }
}