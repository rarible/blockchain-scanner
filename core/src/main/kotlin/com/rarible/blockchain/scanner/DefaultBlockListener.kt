package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.logging.LoggerContext
import com.rarible.core.logging.LoggingUtils
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

class DefaultBlockListener<OB : BlockchainBlock, OL : BlockchainLog, B : Block, L : Log>(
    blockchainClient: BlockchainClient<OB, OL>,
    subscribers: List<LogEventSubscriber<OL, OB>>,
    private val blockService: BlockService<B>,
    logMapper: LogMapper<OL, OB, L>,
    logService: LogService<L>,
    logEventListeners: List<LogEventListener<L>>,
    pendingLogService: PendingLogService<OB, L>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>?,
    private val properties: BlockchainScannerProperties
) : BlockListener {

    private val blockEventHandler: BlockEventHandler<OB, OL, L> = BlockEventHandler(
        blockchainClient,
        subscribers,
        logMapper,
        logEventListeners,
        logService,
        pendingLogService,
        properties.maxAttempts,
        properties.minBackoff
    )

    override fun onBlockEvent(event: BlockEvent): Mono<Void> {
        return LoggingUtils.withMarker { marker: Marker? ->
            logger.info(marker, "onBlockEvent {}", event)
            blockEventHandler.onBlockEvent(event)
                .collectList()
                .flatMap { postProcessLogs(it).thenReturn(Block.Status.SUCCESS) }
                .timeout(Duration.ofMillis(properties.maxProcessTime))
                .onErrorResume {
                    logger.error(marker, "Unable to handle event $event", it)
                    Mono.just<Block.Status?>(Block.Status.ERROR)
                }
                .flatMap { blockService.updateBlockStatus(event.block.number, it) }
                .then()
                .onErrorResume {
                    logger.error(marker, "Unable to save block status $event", it)
                    Mono.empty()
                }
        }.subscriberContext {
            LoggerContext.addToContext(it, event.contextParams)
        }
    }

    private fun postProcessLogs(logs: List<L>): Mono<Void> {
        return Flux.fromIterable(logEventPostProcessors ?: emptyList())
            .flatMap { it.postProcessLogs(logs) }
            .then()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DefaultBlockListener::class.java)
    }
}