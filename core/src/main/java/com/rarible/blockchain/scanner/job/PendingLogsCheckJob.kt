package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.client.BlockchainClient
import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.model.NewBlockEvent
import com.rarible.blockchain.scanner.service.BlockchainListenerService
import com.rarible.blockchain.scanner.service.LogEventService
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import io.daonomic.rpc.domain.Word
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import scalether.domain.response.Block

@Service
class PendingLogsCheckJob<L : LogEvent>(
    private val logEventService: LogEventService<L>,
    descriptors: List<LogEventSubscriber<*, *, *>>,
    private val blockchainClient: BlockchainClient<*, *>,
    private val blockchainListenerService: BlockchainListenerService<*, *, *, *, *>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>? = null
) {
    private val collections = descriptors.map { it.collection }.toSet()

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
                val newBlocks = logsAndBlocks.mapNotNull { it.second }.distinctBy { it.hash() }
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

    private fun onNewBlocks(newBlocks: List<Block<Word>>): Mono<Void> =
        newBlocks.toFlux().flatMap { block ->
            blockchainListenerService.onBlock(
                NewBlockEvent(
                    block.number().toLong(),
                    block.hash().toString(), // TODO ???
                    block.timestamp().toLong(),
                    null
                )
            )
        }.then()

    private fun processLog(collection: String, log: L) =
        ethereum.ethGetTransactionByHash(log.transactionHash)
            .flatMap { txOption ->
                if (txOption.isEmpty) {
                    logger.info("for log $log\nnot found transaction. dropping it")
                    markLogAsDropped(log, collection)
                        .map { updatedLog -> Pair(updatedLog, null) }
                } else {
                    val tx = txOption.get()
                    val blockHash = tx.blockHash()
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