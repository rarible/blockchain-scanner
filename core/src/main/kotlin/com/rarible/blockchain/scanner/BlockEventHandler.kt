package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.util.retry.Retry
import java.time.Duration
import java.util.stream.Collectors

class BlockEventHandler<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    blockchainClient: BlockchainClient<OB, OL>,
    subscribers: List<LogEventSubscriber<OL, OB>>,
    logMapper: LogMapper<OL, OB, L>,
    logEventListeners: List<LogEventListener<L>>,
    logService: LogService<L>,
    pendingLogService: PendingLogService<OB, L>,
    maxAttempts: Long,
    minBackoff: Long
) {

    private val subscribers = ArrayList<BlockEventSubscriber<OB, OL, L>>()

    private val pendingLogMarker = PendingLogMarker(
        logService,
        pendingLogService
    )

    companion object {
        val logger: Logger = LoggerFactory.getLogger(BlockEventHandler::class.java)
    }

    init {
        logger.info("injected subscribers: {}", subscribers)

        val backoff = Retry.backoff(maxAttempts, Duration.ofMillis(minBackoff))
        for (subscriber in subscribers) {
            val topic = subscriber.getDescriptor().topic
            val logEventTopicListeners: List<LogEventListener<L>> = logEventListeners.stream()
                .filter { it.topics.contains(topic) }
                .collect(Collectors.toList())

            this.subscribers.add(
                BlockEventSubscriber(
                    blockchainClient,
                    subscriber,
                    logMapper,
                    logEventTopicListeners,
                    logService,
                    pendingLogMarker,
                    backoff
                )
            )
        }
    }

    fun onBlockEvent(event: BlockEvent): Flux<L> {
        return Flux.fromIterable(subscribers)
            .flatMap { it.onBlockEvent(event) }
    }

}
