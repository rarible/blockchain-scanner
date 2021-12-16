package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
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
class BlockEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    val subscriber: LogEventSubscriber<BB, BL, L, R, D>,
    private val logService: LogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val logHandler = LogEventHandler(subscriber, logService)

    private val descriptor = subscriber.getDescriptor()
    private val name = subscriber.javaClass.simpleName

    suspend fun onNewBlockEvents(events: List<NewBlockEvent>): List<LogEvent<L, R, D>> {
        val newBlockLogs = getBlockLogs(events)
        return events.mapNotNull { event ->
            val fullBlock = newBlockLogs[event] ?: return@mapNotNull null
            val newRecords = logHandler.handleLogs(fullBlock)
            logService.revertPendingLogs(descriptor, fullBlock)
            logger.info("{} handled for subscriber {}: {} new logs have been gathered", event, name, newRecords.size)
            LogEvent(event, newRecords, descriptor)
        }
    }

    suspend fun onRevertedBlockEvents(events: List<RevertedBlockEvent>): List<LogEvent<L, R, D>> {
        return events.map { event ->
            val reverted = withSpan("revert") {
                logHandler.revert(event)
            }
            logger.info(
                "RevertedBlockEvent [{}] handled for subscriber {}, {} reverted logs has been gathered",
                event, name, reverted.size
            )
            LogEvent(event, reverted, descriptor)
        }
    }

    suspend fun onReindexBlockEvents(events: List<ReindexBlockEvent>): List<LogEvent<L, R, D>> {
        val fetchedLogs = getBlockLogs(events)
        return events.mapNotNull { event ->
            val newLogs = fetchedLogs[event]?.let { logHandler.handleLogs(it) } ?: return@mapNotNull null
            logger.info(
                "ReindexBlockEvent [{}] handled for subscriber {}, {} re-indexed logs has been gathered",
                event, name, newLogs.size
            )
            LogEvent(event, newLogs, descriptor)
        }
    }

    private suspend fun getBlockLogs(events: List<BlockEvent>): Map<BlockEvent, FullBlock<BB, BL>> = coroutineScope {
        val blockNumbers = events.map { it.number }
        val blocksByNumber = events.associateBy { it.number }

        // Ideally, we should have here just one range, since events of blocks are ordered in Kafka
        val ranges = BlockRanges.toRanges(blockNumbers)
        logger.info("Searching for LogEvents in Blocks {} (ranges={})", blockNumbers, ranges)

        val logEvents = logTime("blockchainClient::getBlockEvents") {
            ranges.map { range ->
                async {
                    withSpan("getBlockEvents", "network") {
                        val result = blockchainClient.getBlockLogs(descriptor, range).toList()
                        logger.info("Found {} LogEvents for subscriber {} in Block range {}", result.size, name, range)
                        result
                    }
                }
            }.awaitAll().flatten()
        }
        logEvents.associateBy { blocksByNumber[it.block.number]!! }
    }

    override fun toString(): String {
        return subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
    }
}
