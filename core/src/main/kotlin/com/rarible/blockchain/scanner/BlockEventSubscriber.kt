package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flowOf
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    val subscriber: LogEventSubscriber<BB, BL, L, R, D>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    private val pendingLogMarker: PendingLogMarker<BB, L, R, D>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService)

    fun onBlockEvent(event: BlockEvent): Flow<R> = flatten {
        val start = logHandler.beforeHandleBlock(event)
        val descriptor = subscriber.getDescriptor()
        logger.debug("Handling BlockEvent [{}] for subscriber with descriptor: [{}]", event, descriptor)

        val block = blockchainClient.getBlock(event.block.hash)

        flowOf(
            start,
            pendingLogMarker.markInactive(block, descriptor),
            processBlock(block)
        ).flattenConcat()
    }

    private fun processBlock(originalBlock: BB): Flow<R> = flatten {
        val events = blockchainClient.getBlockEvents(originalBlock, subscriber.getDescriptor())
        logHandler.handleLogs(FullBlock(originalBlock, events))
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}



