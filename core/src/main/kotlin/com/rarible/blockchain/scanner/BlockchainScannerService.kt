package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.core.logging.LoggerContext.addToContext
import com.rarible.core.logging.LoggingUtils
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import javax.annotation.PostConstruct

@Component
class BlockchainScannerService<OB : BlockchainBlock, OL, B : Block, L : LogEvent, D : EventData>(
    private val blockEventHandler: BlockEventHandler<OB, OL, L, D>,
    private val blockService: BlockService<B>,
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>?,
    private val blockScanner: BlockScanner<OB, OL, B>,

    @Value("\${ethereumMaxProcessTime:300000}") private val maxProcessTime: Long
) {
    @PostConstruct
    fun init() {
        Mono.delay(Duration.ofMillis(1000))
            .thenMany(blockScanner.scan())
            .map { toNewBlockEvent(it) }
            .timeout(Duration.ofMinutes(5))
            .concatMap { onBlock(it) }
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

    private fun toNewBlockEvent(event: BlockEvent<OB>): NewBlockEvent {
        val block: BlockchainBlock = event.block
        return NewBlockEvent(
            block.number,
            block.hash,  // TODO ???
            block.timestamp,
            if (event.reverted != null) event.reverted.hash else null // TODO There was Word type
        )
    }

    // TODO Should be in separate class
    fun reindexPendingBlock(block: B): Mono<Void?> {
        return LoggingUtils.withMarker { marker: Marker? ->
            logger.info(marker, "reindexing block {}", block)
            blockchainClient.getBlock(block.id).flatMap {
                val event = NewBlockEvent(block.id, it.hash, it.timestamp, null)
                onBlock(event)
            }
        }
    }

    fun onBlock(event: NewBlockEvent): Mono<Void?> {
        return LoggingUtils.withMarker { marker: Marker? ->
            logger.info(marker, "onBlockEvent {}", event)
            onBlockEvent(event)
                .collectList()
                .flatMap { postProcessLogs(it).thenReturn(Block.Status.SUCCESS) }
                .timeout(Duration.ofMillis(maxProcessTime))
                .onErrorResume {
                    logger.error(marker, "Unable to handle event $event", it)
                    Mono.just<Block.Status?>(Block.Status.ERROR)
                }
                .flatMap { blockService.updateBlockStatus(event.number, it) }
                .then()
                .onErrorResume {
                    logger.error(marker, "Unable to save block status $event", it)
                    Mono.empty()
                }
        }.subscriberContext {
            addToContext(it, event.contextParams)
        }
    }

    private fun onBlockEvent(event: NewBlockEvent): Flux<L> {
        return blockEventHandler.onBlockEvent(event)
    }

    private fun postProcessLogs(logs: List<L>): Mono<Void> {
        return Flux.fromIterable(logEventPostProcessors ?: emptyList())
            .flatMap { it.postProcessLogs(logs) }
            .then()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BlockchainScannerService::class.java)
    }
}