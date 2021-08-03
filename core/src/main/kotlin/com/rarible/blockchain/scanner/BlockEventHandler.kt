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
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventHandler<BB : BlockchainBlock, BL : BlockchainLog, L : Log>(
    blockchainClient: BlockchainClient<BB, BL>,
    subscribers: List<LogEventSubscriber<BB, BL>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L>,
    pendingLogService: PendingLogService<BB, L>
) {

    private val subscribers = ArrayList<BlockEventSubscriber<BB, BL, L>>()

    private val pendingLogMarker = PendingLogMarker(
        logService,
        pendingLogService
    )

    companion object {
        val logger: Logger = LoggerFactory.getLogger(BlockEventHandler::class.java)
    }

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

    fun onBlockEvent(event: BlockEvent): Flow<L> {
        logger.debug("Triggered block event for [{}] subscribers: {}", subscribers.size, event)
        return subscribers.asFlow()
            .flatMapConcat { it.onBlockEvent(event) }
    }

}
