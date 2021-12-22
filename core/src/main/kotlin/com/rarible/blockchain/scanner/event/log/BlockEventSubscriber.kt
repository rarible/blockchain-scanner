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
import com.rarible.core.apm.SpanType
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory

class BlockEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    val subscriber: LogEventSubscriber<BB, BL, L, R, D>,
    private val logService: LogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(subscriber.javaClass)

    private val descriptor = subscriber.getDescriptor()
    private val name = subscriber.javaClass.simpleName

    suspend fun onNewBlockEvents(events: List<NewBlockEvent>): List<LogEvent<L, R, D>> {
        val blockLogs = getBlockLogs(events)
        return events.mapNotNull { event ->
            val fullBlock = blockLogs[event] ?: return@mapNotNull null
            val logRecordsToInsert = prepareLogsToInsert(fullBlock)
            val logRecordsToRevert = logService.prepareLogsToRevertOnNewBlock(descriptor, fullBlock)
            logger.info(
                "Prepared {} new logs and {} logs to remove for {} of {}",
                logRecordsToInsert.size,
                logRecordsToRevert.size,
                name,
                event,
            )
            LogEvent(
                blockEvent = event,
                descriptor = descriptor,
                logRecordsToInsert = logRecordsToInsert,
                logRecordsToRemove = logRecordsToRevert
            )
        }
    }

    suspend fun prepareLogsToInsert(fullBlock: FullBlock<BB, BL>): List<R> {
        val block = fullBlock.block
        val logs = fullBlock.logs
        if (logs.isEmpty()) {
            logger.info("No logs in the full block {} for {}", block, name)
            return emptyList()
        }
        logger.info("[{}] Preparing log event records for the full block {}", name, block)
        return logs.flatMap { subscriber.getEventRecords(block, it) }
    }

    suspend fun onRevertedBlockEvents(events: List<RevertedBlockEvent>): List<LogEvent<L, R, D>> {
        return events.map { event ->
            val logsToRevert = withSpan("revert") {
                logService.prepareLogsToRevertOnRevertedBlock(descriptor, event.hash).toList()
            }.toList()
            logger.info(
                "Prepared {} logs to revert for subscriber {} and RevertedBlockEvent {}",
                logsToRevert.size, name, event
            )
            LogEvent(
                blockEvent = event,
                descriptor = descriptor,
                logRecordsToInsert = emptyList(),
                logRecordsToRemove = logsToRevert
            )
        }
    }

    suspend fun onReindexBlockEvents(events: List<ReindexBlockEvent>): List<LogEvent<L, R, D>> {
        val blockLogs = getBlockLogs(events)
        return events.mapNotNull { event ->
            val fullBlock = blockLogs[event] ?: return@mapNotNull null
            val newLogs = prepareLogsToInsert(fullBlock)
            logger.info(
                "ReindexBlockEvent [{}] handled for subscriber {}, {} re-indexed logs has been gathered",
                event, name, newLogs.size
            )
            LogEvent(
                blockEvent = event,
                descriptor = descriptor,
                logRecordsToInsert = newLogs,
                logRecordsToRemove = emptyList()
            )
        }
    }

    private suspend fun getBlockLogs(events: List<BlockEvent>): Map<BlockEvent, FullBlock<BB, BL>> = coroutineScope {
        val blockNumbers = events.map { it.number }
        val blocksByNumber = events.associateBy { it.number }

        // Ideally, we should have here just one range, since events of blocks are ordered in Kafka
        val blockRanges = BlockRanges.toRanges(blockNumbers)
        logger.info("Searching for {} logs in block ranges {}", name, blockRanges)

        blockRanges.map { range ->
            async {
                withSpan("getBlockLogs", SpanType.EXT) {
                    val blockLogs = blockchainClient.getBlockLogs(descriptor, range).toList()
                    logger.info("Found {} logs for {} in range {}", blockLogs.size, name, range)
                    blockLogs
                }
            }
        }.awaitAll().flatten().associateBy { blocksByNumber.getValue(it.block.number) }
    }

    override fun toString(): String = subscriber::class.java.name + ":[${subscriber.getDescriptor()}]"
}
