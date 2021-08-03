package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log, D : LogEventDescriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    val subscriber: LogEventSubscriber<BB, BL, D>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, D>,
    private val pendingLogMarker: PendingLogMarker<BB, L, D>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService)

    fun onBlockEvent(event: BlockEvent): Flow<L> = flatten {
        val start = logHandler.beforeHandleBlock(event)
        val descriptor = subscriber.getDescriptor()
        logger.debug("Handling BlockEvent [{}] for subscriber with descriptor: [{}]", event, descriptor)

        val block = blockchainClient.getBlock(event.block.hash)

        //todo тут можно все merge вместе сделать? все 3 потока. результат не изменится
        //todo но кажется это не совсем верно, потому что вначале pending нужно убрать, а потом из блоков грузить
        //todo иначе получится, что индексы уникальные могут сказать ошибку
        val process = merge(
            pendingLogMarker.markInactive(block, descriptor),
            processBlock(block)
        )

        merge(start, process)
    }

    private fun processBlock(originalBlock: BB): Flow<L> = flatten {
        val events = blockchainClient.getBlockEvents(originalBlock, subscriber.getDescriptor())
        logHandler.handleLogs(FullBlock(originalBlock, events))
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}



