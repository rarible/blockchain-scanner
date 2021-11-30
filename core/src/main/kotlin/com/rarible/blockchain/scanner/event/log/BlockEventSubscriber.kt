package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.blockchain.scanner.util.logTime
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    val subscriber: LogEventSubscriber<BB, BL, L, R, D>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    private val pendingLogMarker: PendingLogMarker<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logMapper, logService)

    private val descriptor = subscriber.getDescriptor()
    private val name = subscriber.javaClass

    suspend fun onRevertedBlockEvents(events: List<RevertedBlockEvent>): Map<BlockEvent, List<R>> {
        return events.associateBy({ it }, { event ->
            val deletedAndReverted = beforeHandleBlock(event)
            logger.info(
                "RevertedBlockEvent [{}] handled for subscriber {}, {} pending/deleted/reverted logs has been gathered",
                event, name, deletedAndReverted.size
            )
            deletedAndReverted
        })
    }

    suspend fun onNewBlockEvents(events: List<NewBlockEvent>): Map<BlockEvent, List<R>> {
        val futureLogs = coroutineScope { async { getLogEvents(events) } }
        val deletedAndPending = events.associateBy(
            { it.hash }, { beforeHandleBlock(it) }
        )
        val fetchedLogs = futureLogs.await()

        return events.associateBy({ it }, { event ->
            val beforeHandleLogs = deletedAndPending.getOrDefault(event.hash, emptyList())
            val newLogs = fetchedLogs[event.hash]?.let { processLogs(it) } ?: emptyList()
            logger.info(
                "NewBlockEvent [{}] handled for subscriber {}, {} pending/deleted/reverted log and {} new logs has been gathered",
                event, name, beforeHandleLogs.size, newLogs.size
            )
            beforeHandleLogs + newLogs
        })
    }

    private suspend fun beforeHandleBlock(event: BlockEvent): List<R> {
        val deletedAndReverted = logTime("logHandler.beforeHandleBlock [${event.number}]") {
            withSpan("beforeHandleBlock") {
                logHandler.beforeHandleBlock(event)
            }
        }
        val pending = logTime("pendingLogMarker.markInactive [${event.number}]") {
            withSpan("markInactive", "db") {
                pendingLogMarker.markInactive(event.hash, descriptor)
            }
        }
        return deletedAndReverted + pending
    }

    private suspend fun processLogs(fullBlock: FullBlock<BB, BL>): List<R> {
        return logTime("logHandler::handleLogs [${fullBlock.block.number}]") {
            logHandler.handleLogs(fullBlock)
        }
    }

    private suspend fun getLogEvents(events: List<NewBlockEvent>): Map<String, FullBlock<BB, BL>> = coroutineScope {
        val blockNumbers = events.map { it.number }
        val ranges = BlockRanges.toRanges(blockNumbers)
        logger.info("Searching for LogEvents in Blocks {} (ranges={})", blockNumbers, ranges)

        val logEvents = logTime("blockchainClient::getBlockEvents") {
            ranges.map { range ->
                async {
                    withSpan("getBlockEvents", "network") {
                        val result = blockchainClient.getBlockEvents(descriptor, range).toList()
                        logger.info("Found {} LogEvents for subscriber {} in Block range {}", result.size, name, range)
                        result
                    }
                }
            }.awaitAll().flatten()
        }
        logEvents.associateBy { it.block.hash }
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}



