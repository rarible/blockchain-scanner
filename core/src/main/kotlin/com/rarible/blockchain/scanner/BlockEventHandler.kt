package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventHandler<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    pendingLogService: PendingLogService<BB, L, R, D>
) {

    private val logger = LoggerFactory.getLogger(BlockEventHandler::class.java)

    private val subscribers = ArrayList<BlockEventSubscriber<BB, BL, L, R, D>>()

    private val pendingLogMarker = PendingLogMarker(
        logService,
        pendingLogService
    )

    init {
        logger.info("Injecting {} subscribers", subscribers.size)

        for (subscriber in subscribers) {
            val blockEventSubscriber = BlockEventSubscriber(
                blockchainClient,
                subscriber,
                logMapper,
                logService,
                pendingLogMarker
            )
            this.subscribers.add(blockEventSubscriber)

            logger.info("Injected {} subscriber into BlockEventHandler", blockEventSubscriber)
        }
    }

    fun onBlockEvent(event: BlockEvent): Flow<R> {
        logger.debug("Triggered block event for [{}] subscribers: {}", subscribers.size, event)
        return subscribers.asFlow()
            .flatMapConcat {
                withSpan("processSingleSubscriber", labels = listOf("subscriber" to it.subscriber.javaClass.name)) {
                    it.onBlockEvent(event)
                }
            }
    }

}
