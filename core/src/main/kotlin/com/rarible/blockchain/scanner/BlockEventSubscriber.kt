package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.util.retry.RetryBackoffSpec

class BlockEventSubscriber<OB : BlockchainBlock, OL : BlockchainLog, L : Log, D : EventData>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    val subscriber: LogEventSubscriber<OL, OB, D>,
    logMapper: LogMapper<OL, OB, L>,
    logEventListeners: List<LogEventListener<L>>,
    logService: LogService<L>,
    private val pendingLogMarker: PendingLogMarker<OB, L>,
    private val backoff: RetryBackoffSpec
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService, logEventListeners)

    fun onBlockEvent(event: BlockEvent): Flux<L> {
        val start = logHandler.beforeHandleBlock(event)

        val descriptor = subscriber.getDescriptor()

        val originalBlock = blockchainClient.getBlock(event.block.hash)
            .doOnError { th -> logger.warn("Unable to get block by hash: " + event.block.hash, th) }
            .retryWhen(backoff)

        val process = originalBlock.flatMapMany {
            Flux.concat(
                pendingLogMarker.markInactive(it, descriptor),
                processBlock(it)
            )
        }

        return Flux.concat(start, process)
    }

    private fun processBlock(originalBlock: OB): Flux<L> {
        return LoggingUtils.withMarkerFlux { marker ->
            blockchainClient.getBlockEvents(originalBlock, subscriber.getDescriptor(), marker)
                .flatMapMany { logHandler.handleLogs(marker, originalBlock, it) }
        }
    }

}



