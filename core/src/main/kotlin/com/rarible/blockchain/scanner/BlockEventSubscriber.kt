package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.logTime
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
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

    private val logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService)

    suspend fun onBlockEvent(event: BlockEvent): List<R> {
        val start = logTime("logHandler.beforeHandleBlock [${event.number}]") {
            withSpan("beforeHandleBlock") {
                logHandler.beforeHandleBlock(event)
            }
        }
        val descriptor = subscriber.getDescriptor()
        logger.debug("Handling BlockEvent [{}] for subscriber with descriptor: [{}]", event, descriptor)

        val block = logTime("Get block [${event.number}] from handler") {
            withSpan("getBlock", "network") { blockchainClient.getBlock(event.number) }
        }

        val pending = logTime("pendingLogMarker.markInactive [${event.number}]") {
            withSpan("markInactive", "db") { pendingLogMarker.markInactive(block, descriptor) }
        }

        return start + pending + processBlock(block)

    }

    private suspend fun processBlock(originalBlock: BB): List<R> {
        val events = logTime("blockchainClient::getBlockEvents [${originalBlock.number}]") {
            withSpan("getBlockEvents", "network") {
                // TODO may be launched async while we gathering reverted/pending logs
                blockchainClient.getBlockEvents(subscriber.getDescriptor(), originalBlock).toList()
            }
        }
        return logTime("logHandler::handleLogs [${originalBlock.number}]") {
            withSpan("handleLogs") { logHandler.handleLogs(FullBlock(originalBlock, events)) }
        }
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}



