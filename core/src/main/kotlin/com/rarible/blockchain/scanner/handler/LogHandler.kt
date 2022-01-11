package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.apm.SpanType
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory

class LogHandler<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    private val logService: LogService<R, D>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val logRecordEventPublisher: LogRecordEventPublisher,
) : BlockEventListener<BB> {

    private val logger = LoggerFactory.getLogger(LogHandler::class.java)

    override suspend fun process(events: List<BlockEvent<BB>>) {
        logger.info("Processing BlockEvents: {}", events)
        val logEvents = withSpan("prepareBlockEventsBatch") {
            val batches = BlockRanges.toBatches(events)
            batches.flatMap { prepareBlockEventsBatch(it) }
        }
        val blockLogsToInsert = hashMapOf<BlockEvent<*>, MutableMap<String, MutableList<R>>>()
        val blockLogsToRemove = hashMapOf<BlockEvent<*>, MutableMap<String, MutableList<R>>>()
        for (logEvent in logEvents) {
            blockLogsToInsert.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.groupId) { arrayListOf() } += logEvent.logRecordsToInsert
            blockLogsToRemove.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.groupId) { arrayListOf() } += logEvent.logRecordsToRemove
        }
        for (blockEvent in events) {
            val toInsertGroupIdMap = blockLogsToInsert[blockEvent] ?: continue
            val toRemoveGroupIdMap = blockLogsToRemove.getValue(blockEvent)
            withSpan("publishRecordsToRemove", type = SpanType.KAFKA) {
                publishRecordsToRemove(blockEvent, toRemoveGroupIdMap)
            }
            withSpan("publishRecordsToInsert", type = SpanType.KAFKA) {
                publishRecordsToInsert(blockEvent, toInsertGroupIdMap)
            }
            logger.info("Sent events for {}", blockEvent)
        }
        withSpan("insertOrRemoveRecords", type = SpanType.DB) {
            insertOrRemoveRecords(logEvents)
        }
    }

    private suspend fun publishRecordsToInsert(
        blockEvent: BlockEvent<BB>,
        toInsertGroupIdMap: MutableMap<String, MutableList<R>>
    ) {
        for ((groupId, recordsToInsert) in toInsertGroupIdMap) {
            logger.info(
                "Publishing {} log records to insert for {} of {}",
                recordsToInsert.size,
                groupId,
                blockEvent
            )
            if (recordsToInsert.isNotEmpty()) {
                val logRecordEvents = recordsToInsert.sortedWith(logRecordComparator)
                    .map { LogRecordEvent(it, false) }
                logRecordEventPublisher.publish(groupId, logRecordEvents)
            }
        }
    }

    private suspend fun publishRecordsToRemove(
        blockEvent: BlockEvent<BB>,
        toRemoveGroupIdMap: MutableMap<String, MutableList<R>>
    ) {
        for ((groupId, recordsToRemove) in toRemoveGroupIdMap) {
            logger.info(
                "Publishing {} log records to remove for {} of {}",
                recordsToRemove.size,
                groupId,
                blockEvent
            )
            if (recordsToRemove.isNotEmpty()) {
                val logRecordEvents = recordsToRemove.sortedWith(logRecordComparator)
                    .map { LogRecordEvent(it, true) }
                logRecordEventPublisher.publish(groupId, logRecordEvents)
            }
        }
    }

    private suspend fun prepareBlockEventsBatch(batch: List<BlockEvent<BB>>): List<LogEvent<R, D>> =
        coroutineScope {
            subscribers.map { subscriber ->
                async {
                    withSpan(
                        name = "processSubscriber",
                        labels = listOf("descriptor" to subscriber.getDescriptor().id)
                    ) {
                        @Suppress("UNCHECKED_CAST")
                        when (batch[0]) {
                            is NewStableBlockEvent -> onBlock(subscriber, batch as List<NewBlockEvent<BB>>, true)
                            is NewUnstableBlockEvent -> onBlock(subscriber, batch as List<NewBlockEvent<BB>>, false)
                            is RevertedBlockEvent -> onRevertedBlocks(subscriber, batch as List<RevertedBlockEvent<BB>>)
                        }
                    }
                }
            }.awaitAll().flatten()
        }

    private suspend fun insertOrRemoveRecords(logEvents: List<LogEvent<R, D>>) = coroutineScope {
        logEvents.flatMap { logEvent ->
            listOf(
                async {
                    logService.delete(logEvent.descriptor, logEvent.logRecordsToRemove)
                },
                async {
                    logService.save(logEvent.descriptor, logEvent.logRecordsToInsert)
                }
            )
        }.awaitAll()
    }

    private suspend fun onBlock(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        events: List<NewBlockEvent<BB>>,
        stable: Boolean
    ): List<LogEvent<R, D>> {
        val blockLogs = withSpan(
            name = "getBlockLogs",
            type = SpanType.EXT,
            labels = listOf("stable" to stable)
        ) {
            blockchainClient.getBlockLogs(
                descriptor = subscriber.getDescriptor(),
                blocks = events.map { it.block },
                stable = stable
            ).toList()
        }.associateBy { it.block.number }

        // Preserve the order of events.
        return events.mapNotNull { event ->
            val fullBlock = blockLogs[event.number] ?: return@mapNotNull null
            val logRecordsToInsert = withSpan(name = "prepareLogsToInsert") {
                prepareLogsToInsert(subscriber, fullBlock)
            }
            val logRecordsToRevert = withSpan(name = "prepareLogsToRevert") {
                logService.prepareLogsToRevertOnNewBlock(subscriber.getDescriptor(), fullBlock)
            }
            logger.info(
                "Prepared {} new logs and {} logs to remove for {} of {}",
                logRecordsToInsert.size,
                logRecordsToRevert.size,
                subscriber.getDescriptor().id,
                event
            )
            LogEvent(
                blockEvent = event,
                descriptor = subscriber.getDescriptor(),
                logRecordsToInsert = logRecordsToInsert,
                logRecordsToRemove = logRecordsToRevert
            )
        }
    }

    private suspend fun prepareLogsToInsert(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        fullBlock: FullBlock<BB, BL>
    ): List<R> {
        if (fullBlock.logs.isEmpty()) {
            logger.info(
                "No logs in the full block [{}:{}] for {}",
                fullBlock.block.number,
                fullBlock.block.hash,
                subscriber.getDescriptor().id
            )
            return emptyList()
        }
        logger.info(
            "Preparing log event records for the full block [{}:{}] for {}",
            fullBlock.block.number,
            fullBlock.block.hash,
            subscriber.getDescriptor().id
        )
        return fullBlock.logs.flatMap { subscriber.getEventRecords(fullBlock.block, it) }
    }

    private suspend fun onRevertedBlocks(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        events: List<RevertedBlockEvent<BB>>
    ): List<LogEvent<R, D>> = events.map { event ->
        val toRevertLogs = withSpan(name = "prepareLogsToRevert") {
            logService.prepareLogsToRevertOnRevertedBlock(
                descriptor = subscriber.getDescriptor(),
                revertedBlockHash = event.hash
            ).toList()
        }
        logger.info(
            "Prepared {} logs to revert for subscriber {} and {}",
            toRevertLogs.size,
            subscriber.getDescriptor().id,
            event
        )
        LogEvent(
            blockEvent = event,
            descriptor = subscriber.getDescriptor(),
            logRecordsToInsert = emptyList(),
            logRecordsToRemove = toRevertLogs
        )
    }

}
