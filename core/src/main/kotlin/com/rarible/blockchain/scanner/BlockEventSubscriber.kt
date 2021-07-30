package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventSubscriber<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    val subscriber: LogEventSubscriber<OL, OB>,
    logMapper: LogMapper<OL, OB, L>,
    logEventListeners: List<LogEventListener<L>>,
    logService: LogService<L>,
    private val pendingLogMarker: PendingLogMarker<OB, L>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService, logEventListeners)

    suspend fun onBlockEvent(event: BlockEvent): Flow<L> {
        val start = logHandler.beforeHandleBlock(event)
        val descriptor = subscriber.getDescriptor()
        logger.debug("Handling BlockEvent [{}] for subscriber with descriptor: [{}]", event, descriptor)

        val block = blockchainClient.getBlock(event.block.hash)

        val process = merge(
            pendingLogMarker.markInactive(block, descriptor),
            processBlock(block)
        )

        return merge(start, process)
    }

    private suspend fun processBlock(originalBlock: OB): Flow<L> {
        val events = blockchainClient.getBlockEvents(originalBlock, subscriber.getDescriptor())
        return logHandler.handleLogs(originalBlock, events)
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}



