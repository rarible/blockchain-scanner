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
import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter
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
    private val logFilters: List<LogEventFilter<R, D>>,
    private val logService: LogService<R, D>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val logRecordEventPublisher: LogRecordEventPublisher,
) : BlockEventListener<BB> {

    private val logger = LoggerFactory.getLogger(LogHandler::class.java)

    override suspend fun process(events: List<BlockEvent<BB>>) {
        logger.info(
            "Processing ${events.size} events for group '${subscribers.first().getDescriptor().groupId}': $events"
        )
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
        withSpan(name = "publishEvents") {
            for (blockEvent in events) {
                val toInsertGroupIdMap = blockLogsToInsert[blockEvent] ?: continue
                val toRemoveGroupIdMap = blockLogsToRemove.getValue(blockEvent)
                publishRecordsToRemove(blockEvent, toRemoveGroupIdMap)
                publishRecordsToInsert(blockEvent, toInsertGroupIdMap)
            }
        }
        val toInsert = logEvents.sumOf { it.logRecordsToInsert.size }
        val toDelete = logEvents.sumOf { it.logRecordsToRemove.size }
        withSpan(
            name = "insertOrRemoveRecords",
            type = SpanType.DB,
            labels = listOf("toInsert" to toInsert, "toDelete" to toDelete)
        ) {
            insertOrRemoveRecords(logEvents)
        }
    }

    private suspend fun publishRecordsToInsert(
        blockEvent: BlockEvent<BB>,
        toInsertGroupIdMap: MutableMap<String, MutableList<R>>
    ) {
        for ((groupId, recordsToInsert) in toInsertGroupIdMap) {
            logging(
                message = "publishing ${recordsToInsert.size} new log records",
                event = blockEvent
            )
            if (recordsToInsert.isNotEmpty()) {
                val logRecordEvents = recordsToInsert.sortedWith(logRecordComparator)
                    .map { LogRecordEvent(it, false) }
                logRecordEventPublisher.publish(groupId, logRecordEvents)
            }
        }
    }

    private suspend fun publishRecordsToRemove(
        event: BlockEvent<BB>,
        toRemoveGroupIdMap: MutableMap<String, MutableList<R>>
    ) {
        for ((groupId, recordsToRemove) in toRemoveGroupIdMap) {
            if (recordsToRemove.isNotEmpty()) {
                logging(message = "publishing ${recordsToRemove.size} reverted log records", event = event)
                val logRecordEvents = recordsToRemove.sortedWith(logRecordComparator.reversed())
                    .map { LogRecordEvent(it, true) }
                logRecordEventPublisher.publish(groupId, logRecordEvents)
            }
        }
    }

    private suspend fun prepareBlockEventsBatch(batch: List<BlockEvent<BB>>): List<LogEvent<R, D>> {
        var events = coroutineScope {
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
        if (logFilters.isNotEmpty()) {
            withSpan("filterLogEvents") {
                for (filter in logFilters) {
                    events = filter.filter(events)
                }
            }
        }
        return events
    }

    private suspend fun insertOrRemoveRecords(logEvents: List<LogEvent<R, D>>) = coroutineScope {
        logEvents.map {
            async {
                runRethrowingBlockHandlerException("Delete log records for ${it.blockEvent} by ${it.descriptor.groupId}") {
                    logService.delete(it.descriptor, it.logRecordsToRemove)
                }
            }
        }.awaitAll()
        logEvents.map {
            async {
                runRethrowingBlockHandlerException("Insert log records for ${it.blockEvent} by ${it.descriptor.groupId}") {
                    logService.save(it.descriptor, it.logRecordsToInsert)
                }
            }
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

        val total = events.sumOf {
            blockLogs[it.number]?.logs?.size ?: 0
        }
        return withSpan(
            name = "extractLogEvents",
            labels = listOf("size" to total)
        ) {
            // Preserve the order of events.
            events.mapNotNull { event ->
                val fullBlock = blockLogs[event.number] ?: return@mapNotNull null
                val logRecordsToInsert = prepareLogsToInsert(subscriber, fullBlock, event)
                val logRecordsToRevert = if (!stable) {
                    runRethrowingBlockHandlerException(
                        actionName = "Prepare log records to revert for $event by ${subscriber.getDescriptor().id}"
                    ) { logService.prepareLogsToRevertOnNewBlock(subscriber.getDescriptor(), fullBlock) }
                } else {
                    emptyList()
                }
                logging(
                    message = "prepared ${logRecordsToInsert.size} records to insert " +
                            "and ${logRecordsToRevert.size} records to remove",
                    event = event,
                    subscriber = subscriber
                )
                LogEvent(
                    blockEvent = event,
                    descriptor = subscriber.getDescriptor(),
                    logRecordsToInsert = logRecordsToInsert,
                    logRecordsToRemove = logRecordsToRevert
                )
            }
        }
    }

    private suspend fun prepareLogsToInsert(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        fullBlock: FullBlock<BB, BL>,
        event: NewBlockEvent<BB>
    ): List<R> {
        if (fullBlock.logs.isEmpty()) {
            logging(
                message = "no logs in the block",
                event = event,
                subscriber = subscriber
            )
            return emptyList()
        }
        val logRecords = runRethrowingBlockHandlerException(
            actionName = "Prepare log records to insert for $event by ${subscriber.getDescriptor().id}"
        ) { fullBlock.logs.flatMap { subscriber.getEventRecords(fullBlock.block, it) } }
        logging(
            message = "prepared ${logRecords.size} log records to insert",
            event = event,
            subscriber = subscriber
        )
        return logRecords
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
        logging(
            message = "prepared ${toRevertLogs.size} logs to revert",
            event = event,
            subscriber = subscriber
        )
        LogEvent(
            blockEvent = event,
            descriptor = subscriber.getDescriptor(),
            logRecordsToInsert = emptyList(),
            logRecordsToRemove = toRevertLogs
        )
    }

    private fun logging(
        message: String,
        event: BlockEvent<BB>,
        subscriber: LogEventSubscriber<BB, BL, R, D>? = null
    ) {
        val id = subscriber?.getDescriptor()?.id ?: ("group " + subscribers.first().getDescriptor().groupId)
        logger.info("Logs for $event by '$id': $message")
    }

}
