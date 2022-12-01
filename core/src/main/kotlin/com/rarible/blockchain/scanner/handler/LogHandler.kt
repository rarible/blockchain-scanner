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
import com.rarible.blockchain.scanner.monitoring.LogMonitor
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
    private val groupId: String,
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    private val logFilters: List<LogEventFilter<R, D>>,
    private val logService: LogService<R, D>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val logRecordEventPublisher: LogRecordEventPublisher,
    private val logMonitor: LogMonitor
) : BlockEventListener<BB> {

    private val logger = LoggerFactory.getLogger(LogHandler::class.java)

    override suspend fun process(events: List<BlockEvent<BB>>) {
        if (events.isEmpty()) {
            return
        }
        logger.info(
            "Processing ${events.size} block events ${events.first().number}..${events.last().number} for group '${
                subscribers.first().getDescriptor().groupId
            }'"
        )

        val logEvents = withSpan("prepareBlockEventsBatch") {
            val batches = BlockRanges.toBatches(events)
            batches.flatMap { prepareBlockEventsBatch(it) }
        }

        val toInsert = logEvents.sumOf { it.logRecordsToInsert.size }
        val toUpdate = logEvents.sumOf { it.logRecordsToUpdate.size }
        withSpan(
            name = "insertOrUpdateRecords",
            type = SpanType.DB,
            labels = listOf("toInsert" to toInsert, "toUpdate" to toUpdate)
        ) {
            insertOrUpdateRecords(logEvents)
        }
        if (logRecordEventPublisher.isEnabled()) {
            withSpan(name = "sortAndPublishEvents") {
                sortAndPublishEvents(events, logEvents)
            }
        }
    }

    private suspend fun sortAndPublishEvents(events: List<BlockEvent<BB>>, logEvents: List<LogEvent<R, D>>) {
        val blockLogsToInsert = HashMap<BlockEvent<*>, MutableList<List<R>>>(events.size)
        val blockLogsToUpdate = HashMap<BlockEvent<*>, MutableList<List<R>>>(events.size)
        for (logEvent in logEvents) {
            blockLogsToInsert.getOrPut(logEvent.blockEvent) { ArrayList(subscribers.size) }
                .add(logEvent.logRecordsToInsert)
            blockLogsToUpdate.getOrPut(logEvent.blockEvent) { ArrayList(subscribers.size) }
                .add(logEvent.logRecordsToUpdate)
        }

        val blockEventsToInsert = blockLogsToInsert.mapValues { bySubscriber ->
            bySubscriber.value.flatten()
                .sortedWith(logRecordComparator)
                .map { LogRecordEvent(it, false) }
        }

        val blockEventsToUpdate = blockLogsToUpdate.mapValues { bySubscriber ->
            bySubscriber.value.flatten()
                .sortedWith(logRecordComparator.reversed())
                .map { LogRecordEvent(it, true) }
        }

        withSpan("publishEvents") {
            for (blockEvent in events) {
                val eventsToInsert = blockEventsToInsert[blockEvent] ?: continue
                val eventsToUpdate = blockEventsToUpdate.getValue(blockEvent)

                logging(message = "publishing ${eventsToUpdate.size} reverted log records", event = blockEvent)
                logRecordEventPublisher.publish(groupId, eventsToUpdate)

                logging(message = "publishing ${eventsToInsert.size} new log records", event = blockEvent)
                logRecordEventPublisher.publish(groupId, eventsToInsert)
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

    private suspend fun insertOrUpdateRecords(logEvents: List<LogEvent<R, D>>) = coroutineScope {
        logEvents.mapNotNull {
            if (it.logRecordsToUpdate.isEmpty()) {
                return@mapNotNull null
            }
            async {
                logging(message = "updating ${it.logRecordsToUpdate.size} log records", event = it.blockEvent)
                runRethrowingBlockHandlerException(
                    "Update log records for ${it.blockEvent} by ${it.descriptor.groupId}"
                ) {
                    logService.save(it.descriptor, it.logRecordsToUpdate)
                }
            }
        }.awaitAll()
        logEvents.mapNotNull {
            if (it.logRecordsToInsert.isEmpty()) {
                return@mapNotNull null
            }
            async {
                logging(message = "inserting ${it.logRecordsToInsert.size} log records", event = it.blockEvent)
                runRethrowingBlockHandlerException(
                    "Insert log records for ${it.blockEvent} by ${it.descriptor.groupId}"
                ) {
                    logService.save(it.descriptor, it.logRecordsToInsert)
                }
                logMonitor.onLogsInserted(descriptor = it.descriptor, inserted = it.logRecordsToInsert.size)
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

        val total = events.sumOf { blockLogs[it.number]?.logs?.size ?: 0 }
        return withSpan(
            name = "extractLogEvents",
            labels = listOf("size" to total)
        ) {
            coroutineScope {
                // Preserve the order of events.
                events.mapNotNull { event ->
                    val fullBlock = blockLogs[event.number] ?: return@mapNotNull null
                    async {
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
                                    "and ${logRecordsToRevert.size} records to update",
                            event = event,
                            subscriber = subscriber
                        )
                        LogEvent(
                            blockEvent = event,
                            descriptor = subscriber.getDescriptor(),
                            logRecordsToInsert = logRecordsToInsert,
                            logRecordsToUpdate = logRecordsToRevert
                        )
                    }
                }.awaitAll()
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
            logRecordsToUpdate = toRevertLogs
        )
    }

    private fun logging(
        message: String,
        event: BlockEvent<*>,
        subscriber: LogEventSubscriber<*, *, *, *>? = null
    ) {
        val id = subscriber?.getDescriptor()?.id ?: ("group " + subscribers.first().getDescriptor().groupId)
        logger.info("Logs for $event by '$id': $message")
    }

}
