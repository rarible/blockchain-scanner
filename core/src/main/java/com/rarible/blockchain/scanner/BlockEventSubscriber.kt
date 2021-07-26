package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.mapper.LogEventMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.framework.service.LogEventService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.util.retry.RetryBackoffSpec

class BlockEventSubscriber<OB : BlockchainBlock, OL, L : LogEvent, D : EventData>(
    val subscriber: LogEventSubscriber<OL, OB, D>,
    logEventListeners: List<LogEventListener<L>>,
    private val pendingLogMarker: PendingLogMarker<OB, L>,
    private val logEventService: LogEventService<L>,
    private val blockchainClient: BlockchainClient<OB, OL>,
    logEventMapper: LogEventMapper<OL, OB, L>,
    private val backoff: RetryBackoffSpec
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logProcessor = LogEventProcessor(subscriber, logEventMapper, logEventService, logEventListeners)

    fun onBlockEvent(event: NewBlockEvent): Flux<L> {
        val start = beforeProcessBlock(event)

        val descriptor = subscriber.getDescriptor()

        val originalBlock = blockchainClient.getBlockMeta(event.hash)
            .doOnError { th -> logger.warn("Unable to get block by hash: " + event.hash, th) }
            .retryWhen(backoff)

        val process = originalBlock.flatMapMany {
            Flux.concat(
                pendingLogMarker.markInactive(it, descriptor),
                processBlock(it)
            )
        }

        return Flux.concat(start, process)
    }

    private fun beforeProcessBlock(event: NewBlockEvent): Flux<L> {
        val collection = subscriber.getDescriptor().collection
        val topic = subscriber.getDescriptor().topic

        return if (event.reverted != null) {
            logEventService
                .findAndDelete(collection, event.hash, topic, LogEvent.Status.REVERTED)
                .thenMany(logEventService.findAndRevert(collection, topic, event.reverted))
        } else {
            logEventService.findAndDelete(collection, event.hash, topic, LogEvent.Status.REVERTED)
                .thenMany(Flux.empty())
        }
    }

    private fun processBlock(originalBlock: OB): Flux<L> {
        return LoggingUtils.withMarkerFlux { marker ->
            blockchainClient.getBlockEvents(originalBlock, subscriber.getDescriptor(), marker)
                .flatMapMany { logProcessor.processLogs(marker, originalBlock, it) }
        }
    }

}



