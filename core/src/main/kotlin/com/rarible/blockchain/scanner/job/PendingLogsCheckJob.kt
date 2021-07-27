package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.BlockchainScannerService
import com.rarible.blockchain.scanner.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.framework.service.LogEventService
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

@Service
class PendingLogsCheckJob<L : LogEvent>(
    private val logEventService: LogEventService<L>,
    subscribers: List<LogEventSubscriber<*, *, *>>,
    private val blockchainClient: BlockchainClient<*, *>,
    private val blockchainScannerService: BlockchainScannerService<*, *, *, *, *>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>? = null
) {

    private val collections = subscribers.map { it.getDescriptor().collection }.toSet()

    @Scheduled(
        fixedRateString = "\${pendingLogsCheckJobInterval:${DateUtils.MILLIS_PER_HOUR}}",
        initialDelay = DateUtils.MILLIS_PER_MINUTE
    )
    fun job() {
        collections.toFlux()
            .flatMap { collection ->
                logEventService.findPendingLogs(collection)
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

    private fun onDroppedLogs(droppedLogs: List<L>): Mono<Void> =
        Flux.fromIterable(logEventPostProcessors ?: emptyList())
            .flatMap { logEventsListener ->
                logEventsListener.postProcessLogs(droppedLogs).onErrorResume { th ->
                    logger.error(
                        "caught exception while onDroppedLogs logs of listener: {}",
                        logEventsListener.javaClass, th
                    )
                    Mono.empty()
                }
            }.then()

    private fun onNewBlocks(newBlocks: List<BlockchainBlock>): Mono<Void> =
        newBlocks.toFlux().flatMap { block ->
            blockchainScannerService.onBlock(
                NewBlockEvent(
                    block.number,
                    block.hash, // TODO There was Word
                    block.timestamp,
                    null
                )
            )
        }.then()

    private fun processLog(collection: String, log: L) =
        blockchainClient.getTransactionMeta(log.transactionHash)
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
                        blockchainClient.getBlockMeta(blockHash)
                            .map { block -> Pair(null, block) }
                    }
                }
            }

    private fun markLogAsDropped(log: L, collection: String): Mono<L> {
        return logEventService.updateStatus(collection, log, LogEvent.Status.DROPPED)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingLogsCheckJob::class.java)
    }
}