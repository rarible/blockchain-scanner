package com.rarible.blockchain.scanner.service

import com.rarible.blockchain.scanner.client.BlockchainClient
import com.rarible.blockchain.scanner.mapper.LogEventMapper
import com.rarible.blockchain.scanner.model.EventData
import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.model.NewBlockEvent
import com.rarible.blockchain.scanner.service.pending.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import reactor.core.publisher.Flux
import reactor.util.retry.Retry
import java.time.Duration
import java.util.stream.Collectors

class BlockEventHandlerService<OB, OL, L : LogEvent, D : EventData>(
    logEventMapper: LogEventMapper<OL, OB, L>,
    subscribers: List<LogEventSubscriber<OL, OB, D>>,
    logEventListeners: List<LogEventListener<L>>,
    pendingLogService: PendingLogService<OB, L>,
    logEventService: LogEventService<L>,
    blockchainClient: BlockchainClient<OB, OL>,
    @Value("\${ethereumBackoffMaxAttempts:5}") maxAttempts: Long,
    @Value("\${ethereumBackoffMinBackoff:100}") minBackoff: Long
) {

    private val handlers = ArrayList<BlockEventHandler<OB, OL, L, D>>()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(BlockEventHandlerService::class.java)
    }

    init {
        logger.info("injected subscribers: {}", subscribers)

        val backoff = Retry.backoff(maxAttempts, Duration.ofMillis(minBackoff))
        for (subscriber in subscribers) {
            val topicListeners: List<LogEventListener<L>> = logEventListeners.stream()
                .filter { it.topics.contains(subscriber.topic) }
                .collect(Collectors.toList())

            handlers.add(
                BlockEventHandler(
                    subscriber,
                    topicListeners,
                    pendingLogService,
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
