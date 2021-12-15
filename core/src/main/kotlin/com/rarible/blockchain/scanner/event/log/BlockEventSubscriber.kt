package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
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

    val descriptor = subscriber.getDescriptor()
    private val name = subscriber.javaClass.simpleName

    suspend fun onNewBlockEvents(events: List<NewBlockEvent>): Map<BlockEvent, List<R>> {
        val newBlockLogs = getBlockLogs(events)
        return events.associateBy({ it }, { event ->
            val fullBlock = newBlockLogs[event] ?: return@associateBy emptyList()
            val newLogs = processLogs(fullBlock)
            revertPendingLogs(fullBlock)
            logger.info("{} handled for subscriber {}: {} new logs have been gathered", event, name, newLogs.size)
            newLogs
        })
    }

    suspend fun onRevertedBlockEvents(events: List<RevertedBlockEvent>): Map<BlockEvent, List<R>> {
        return events.associateBy({ it }, { event ->
            val reverted = revert(event)
            logger.info(
                "RevertedBlockEvent [{}] handled for subscriber {}, {} reverted logs has been gathered",
                event, name, reverted.size
            )
            reverted
        })
    }

    suspend fun onReindexBlockEvents(events: List<ReindexBlockEvent>): Map<BlockEvent, List<R>> {
        val fetchedLogs = getBlockLogs(events)
        return events.associateBy({ it }, { event ->
            val newLogs = fetchedLogs[event]?.let { processLogs(it) } ?: emptyList()
            logger.info(
                "ReindexBlockEvent [{}] handled for subscriber {}, {} re-indexed logs has been gathered",
                event, name, newLogs.size
            )
            newLogs
        })
    }

    private suspend fun revert(event: RevertedBlockEvent): List<R> {
        val reverted = logTime("logHandler.revert [${event.number}]") {
            withSpan("revert") {
                logHandler.revert(event)
            }
        }
        return reverted
    }

    private suspend fun revertPendingLogs(fullBlock: FullBlock<BB, BL>) {
        logTime("logService.revertPendingLogs [${fullBlock.block}]") {
            withSpan("markInactive", "db") {
                logService.revertPendingLogs(descriptor, fullBlock)
            }
        }
    }

    private suspend fun processLogs(fullBlock: FullBlock<BB, BL>): List<R> {
        return logTime("logHandler::handleLogs [${fullBlock.block.number}]") {
            logHandler.handleLogs(fullBlock)
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
