package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.block.SubscriberStats
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
import com.rarible.blockchain.scanner.framework.util.addOut
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.common.nowMillis
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import java.util.*

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

    override suspend fun process(events: List<BlockEvent<BB>>): Map<Long, BlockStats> {
        if (events.isEmpty()) {
            return emptyMap()
        }
        logger.info(
            "Processing ${events.size} block events ${events.first().number}..${events.last().number} for group '${
                subscribers.first().getDescriptor().groupId
            }'"
        )
        val logEvents = logMonitor.onPrepareLogs {
            val batches = BlockRanges.toBatches(events)
            batches.flatMap { prepareBlockEventsBatch(it) }
        }
        val saved = logMonitor.onSaveLogs {
            insertOrUpdateRecords(logEvents)
        }
        if (logRecordEventPublisher.isEnabled()) {
            logMonitor.onPublishLogs {
                sortAndPublishEvents(events, saved)
            }
        }
        return gatherStats(logEvents)
    }

    private fun gatherStats(logs: List<LogEvent<R, D>>): Map<Long, BlockStats> {
        val result = HashMap<Long, BlockStats>()
        logs.groupBy { it.blockEvent.number }.forEach {
            val blockNumber = it.key
            val logEvents = it.value

            var inserted = 0
            var updated = 0
            val subscriberStats = TreeMap<String, SubscriberStats>()
            logEvents.forEach { event ->
                val stats = SubscriberStats(
                    event.logRecordsToInsert.size,
                    event.logRecordsToUpdate.size
                )
                subscriberStats[event.descriptor.id] = stats
                inserted += stats.inserted
                updated += stats.updated
            }
            result[blockNumber] = BlockStats(
                nowMillis(),
                inserted,
                updated,
                subscriberStats
            )
        }
        return result
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

        val blockEventsToInsert = blockLogsToInsert.mapValues { (blockEvent, bySubscriber) ->
            bySubscriber.flatten()
                .sortedWith(logRecordComparator)
                .map { LogRecordEvent(it, false, blockEvent.eventTimeMarks) }
        }

        val blockEventsToUpdate = blockLogsToUpdate.mapValues { (blockEvent, bySubscriber) ->
            bySubscriber.flatten()
                .sortedWith(logRecordComparator.reversed())
                .map { LogRecordEvent(it, true, blockEvent.eventTimeMarks) }
        }
        for (blockEvent in events) {
            val eventsToInsert = blockEventsToInsert[blockEvent] ?: continue
            val eventsToUpdate = blockEventsToUpdate.getValue(blockEvent)

            logging(message = "publishing ${eventsToUpdate.size} reverted log records", event = blockEvent)
            logRecordEventPublisher.publish(groupId, addOutMark(eventsToUpdate))

            logging(message = "publishing ${eventsToInsert.size} new log records", event = blockEvent)
            logRecordEventPublisher.publish(groupId, addOutMark(eventsToInsert))
        }
    }

    private fun addOutMark(records: List<LogRecordEvent>): List<LogRecordEvent> {
        return records.map { it.copy(eventTimeMarks = it.eventTimeMarks?.addOut()) }
    }

    private suspend fun prepareBlockEventsBatch(batch: List<BlockEvent<BB>>): List<LogEvent<R, D>> {
        var events = coroutineScope {
            subscribers.map { subscriber ->
                async {
                    @Suppress("UNCHECKED_CAST")
                    when (batch[0]) {
                        is NewStableBlockEvent -> onBlock(subscriber, batch as List<NewBlockEvent<BB>>, true)
                        is NewUnstableBlockEvent -> onBlock(subscriber, batch as List<NewBlockEvent<BB>>, false)
                        is RevertedBlockEvent -> onRevertedBlocks(subscriber, batch as List<RevertedBlockEvent<BB>>)
                    }
                }
            }.awaitAll().flatten()
        }
        if (logFilters.isNotEmpty()) {
            for (filter in logFilters) {
                events = filter.filter(events)
            }
        }
        return events
    }

    private suspend fun insertOrUpdateRecords(logEvents: List<LogEvent<R, D>>) = coroutineScope {
        val updated: List<LogEvent<R, D>> = logEvents.mapNotNull {
            if (it.logRecordsToUpdate.isEmpty()) {
                return@mapNotNull null
            }
            async {
                logging(message = "updating ${it.logRecordsToUpdate.size} log records", event = it.blockEvent)
                runRethrowingBlockHandlerException(
                    "Update log records for ${it.blockEvent} by ${it.descriptor.groupId}"
                ) {
                    val saved = logService.save(it.descriptor, it.logRecordsToUpdate)
                    it.copy(logRecordsToUpdate = saved)
                }
            }
        }.awaitAll()
        val inserted: List<LogEvent<R, D>> = logEvents.mapNotNull {
            if (it.logRecordsToInsert.isEmpty()) {
                return@mapNotNull null
            }
            async {
                logging(message = "inserting ${it.logRecordsToInsert.size} log records", event = it.blockEvent)
                val saved = runRethrowingBlockHandlerException(
                    "Insert log records for ${it.blockEvent} by ${it.descriptor.groupId}"
                ) {
                    logService.save(it.descriptor, it.logRecordsToInsert)
                }
                logMonitor.onLogsInserted(descriptor = it.descriptor, inserted = it.logRecordsToInsert.size)
                it.copy(logRecordsToInsert = saved)
            }
        }.awaitAll()
        updated + inserted
    }

    private suspend fun onBlock(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        events: List<NewBlockEvent<BB>>,
        stable: Boolean
    ): List<LogEvent<R, D>> {
        val blockLogs = blockchainClient.getBlockLogs(
            descriptor = subscriber.getDescriptor(),
            blocks = events.map { it.block },
            stable = stable
        ).toList().associateBy { it.block.number }

        return coroutineScope {
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
        ) {
            val records = fullBlock.logs.flatMap {
                subscriber.getEventRecords(fullBlock.block, it)
            }
            subscriber.postProcess(fullBlock, records)
        }
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
        val toRevertLogs = logService.prepareLogsToRevertOnRevertedBlock(
            descriptor = subscriber.getDescriptor(),
            revertedBlockHash = event.hash
        ).toList()

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
