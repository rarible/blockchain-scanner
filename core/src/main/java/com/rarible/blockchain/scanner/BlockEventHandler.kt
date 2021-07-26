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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import reactor.core.publisher.Flux
import reactor.util.retry.Retry
import java.time.Duration
import java.util.stream.Collectors

class BlockEventHandler<OB : BlockchainBlock, OL, L : LogEvent, D : EventData>(
    logEventMapper: LogEventMapper<OL, OB, L>,
    subscribers: List<LogEventSubscriber<OL, OB, D>>,
    logEventListeners: List<LogEventListener<L>>,
    pendingLogMarker: PendingLogMarker<OB, L>,
    logEventService: LogEventService<L>,
    blockchainClient: BlockchainClient<OB, OL>,
    @Value("\${ethereumBackoffMaxAttempts:5}") maxAttempts: Long,
    @Value("\${ethereumBackoffMinBackoff:100}") minBackoff: Long
) {

    private val handlers = ArrayList<BlockEventSubscriber<OB, OL, L, D>>()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(BlockEventHandler::class.java)
    }

    init {
        logger.info("injected subscribers: {}", subscribers)

        val backoff = Retry.backoff(maxAttempts, Duration.ofMillis(minBackoff))
        for (subscriber in subscribers) {
            val topic = subscriber.getDescriptor().topic
            val topicListeners: List<LogEventListener<L>> = logEventListeners.stream()
                .filter { it.topics.contains(topic) }
                .collect(Collectors.toList())

            handlers.add(
                BlockEventSubscriber(
                    subscriber,
                    topicListeners,
                    pendingLogMarker,
                    logEventService,
                    blockchainClient,
                    logEventMapper,
                    backoff
                )
            )
        }
    }

    fun onBlockEvent(event: NewBlockEvent): Flux<L>? {
        return Flux.fromIterable(handlers)
            .flatMap { it.onBlockEvent(event) }
    }

}
