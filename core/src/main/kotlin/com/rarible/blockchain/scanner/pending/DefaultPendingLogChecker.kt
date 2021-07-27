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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

class DefaultPendingLogChecker<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val blockListener: BlockListener,
    private val collections: Set<String>,
    private val logService: LogService<L>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>? = null
) : PendingLogChecker {

    override fun checkPendingLogs() {
        collections.toFlux()
            .flatMap { collection ->
                logService.findPendingLogs(collection)
                    .flatMap { processLog(collection, it) }
            }
            .collectList()
            .flatMap { logsAndBlocks ->
                val droppedLogs = logsAndBlocks.mapNotNull { it.first }
                val newBlocks = logsAndBlocks.mapNotNull { it.second }.distinctBy { it.hash }
                Mono.`when`(
                    onDroppedLogs(droppedLogs),
                    onNewBlocks(newBlocks)
                )
            }
            .block()
    }

    private fun onDroppedLogs(droppedLogs: List<L>): Mono<Void> {
        return Flux.fromIterable(logEventPostProcessors ?: emptyList())
            .flatMap { logEventsListener ->
                logEventsListener.postProcessLogs(droppedLogs).onErrorResume { th ->
                    logger.error(
                        "caught exception while onDroppedLogs logs of listener: {}",
                        logEventsListener.javaClass, th
                    )
                    Mono.empty()
                }
            }.then()
    }

    private fun onNewBlocks(newBlocks: List<BlockchainBlock>): Mono<Void> =
        newBlocks.toFlux().flatMap {
            blockListener.onBlockEvent(BlockEvent(it))
        }.then()

    private fun processLog(collection: String, log: L): Mono<Pair<L?, BlockchainBlock?>> {
        return blockchainClient.getTransactionMeta(log.transactionHash)
            .flatMap { txOption ->
                if (!txOption.isPresent) {
                    logger.info("for log $log\nnot found transaction. dropping it")
                    markLogAsDropped(log, collection)
                        .map { updatedLog -> Pair(updatedLog, null) }
                } else {
                    val tx = txOption.get()
                    val blockHash = tx.blockHash
                    if (blockHash == null) {
                        logger.info("for log $log\nfound transaction $tx\nit's pending. skip it")
                        Mono.empty()
                    } else {
                        logger.info("for log $log\nfound transaction $tx\nit's confirmed. update logs for its block")
                        blockchainClient.getBlock(blockHash)
                            .map { block -> Pair(null, block) }
                    }
                }
            }
    }

    private fun markLogAsDropped(log: L, collection: String): Mono<L> {
        return logService.updateStatus(collection, log, Log.Status.DROPPED)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingLogsCheckJob::class.java)
    }
}