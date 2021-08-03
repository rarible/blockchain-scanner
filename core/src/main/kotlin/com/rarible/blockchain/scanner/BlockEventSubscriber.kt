package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<BB, BL>,
    val subscriber: LogEventSubscriber<BB, BL>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L>,
    private val pendingLogMarker: PendingLogMarker<BB, L>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService)

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

    private suspend fun processBlock(originalBlock: BB): Flow<L> {
        val events = blockchainClient.getBlockEvents(originalBlock, subscriber.getDescriptor())
        return logHandler.handleLogs(FullBlock(originalBlock, events))
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}



