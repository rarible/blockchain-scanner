package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.pending.DefaultPendingBlockChecker
import com.rarible.blockchain.scanner.pending.DefaultPendingLogChecker
import com.rarible.blockchain.scanner.pending.PendingBlockChecker
import com.rarible.blockchain.scanner.pending.PendingLogChecker
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

open class BlockchainScanner<OB : BlockchainBlock, OL : BlockchainLog, B : Block, L : Log>(
    blockchainClient: BlockchainClient<OB, OL>,
    subscribers: List<LogEventSubscriber<OL, OB>>,
    blockMapper: BlockMapper<OB, B>,
    blockService: BlockService<B>,
    logMapper: LogMapper<OL, OB, L>,
    logService: LogService<L>,
    logEventListeners: List<LogEventListener<L>>,
    pendingLogService: PendingLogService<OB, L>,
    logEventPostProcessors: List<LogEventPostProcessor<L>>?,
    properties: BlockchainScannerProperties

) : PendingLogChecker, BlockListener, PendingBlockChecker, ReconciliationExecutor {

    private val blockScanner = DefaultBlockScanner(
        blockchainClient,
        blockMapper,
        blockService
    )

    private val blockListener = DefaultBlockListener(
        blockchainClient,
        subscribers,
        blockService,
        logMapper,
        logService,
        logEventListeners,
        pendingLogService,
        logEventPostProcessors,
        properties
    )

    private val pendingLogChecker = DefaultPendingLogChecker(
        blockchainClient,
        blockListener,
        subscribers.map { it.getDescriptor().collection }.toSet(),
        logService,
        logEventPostProcessors
    )

    private val pendingBlockChecker = DefaultPendingBlockChecker(
        blockchainClient,
        blockService,
        blockListener
    )

    private val reconciliationService = ReconciliationService(
        blockchainClient,
        subscribers,
        logMapper,
        logService,
        logEventListeners,
        properties
    )

    private val topics = subscribers.map { it.getDescriptor().topic }.toSet()

    fun scan() {
        Mono.delay(Duration.ofMillis(1000))
            .thenMany(blockScanner.scan())
            .timeout(Duration.ofMinutes(5))
            .concatMap { onBlockEvent(it) }
            .then(Mono.error<Void>(IllegalStateException("disconnected")))
            .retryWhen(
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(300))
                    .maxBackoff(Duration.ofMillis(2000))
                    .doAfterRetry { logger.warn("retrying {}", it) }
            )
            .subscribe(
                { },
                { logger.error("unable to process block events. should never happen", it) }
            )
    }

    override fun onBlockEvent(event: BlockEvent): Mono<Void> {
        return blockListener.onBlockEvent(event)
    }

    override fun checkPendingLogs() {
        pendingLogChecker.checkPendingLogs()
    }

    override fun checkPendingBlocks() {
        pendingBlockChecker.checkPendingBlocks()
    }

    override fun reconcile(topic: String?, from: Long): Flux<LongRange> {
        return reconciliationService.reindex(topic, from)
    }

    override fun getTopics(): Set<String> {
        return topics
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BlockchainScanner::class.java)
    }
}