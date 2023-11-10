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
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.framework.util.addScannerOut
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.common.asyncWithTraceId
import com.rarible.core.common.nowMillis
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import java.util.TreeMap

class LogHandler<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    override val groupId: String,
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    private val logService: LogService<R, D>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val logRecordEventPublisher: LogRecordEventPublisher,
    private val logMonitor: LogMonitor
) : BlockEventListener<BB> {

    private val logger = LoggerFactory.getLogger(LogHandler::class.java)

    override suspend fun process(events: List<BlockEvent<BB>>, mode: ScanMode): BlockListenerResult {
        events.ifEmpty { return BlockListenerResult.EMPTY }

        val range = LongRange(events.first().number, events.last().number)
        logger.info("Processing ${events.size} block events $range for group '$groupId'")

        val logEvents = logMonitor.onPrepareLogs {
            BlockRanges.toBatches(events).flatMap { prepareBlockEventsBatch(it, mode) }
        }

        val failed = logEvents.filterIsInstance<SubscriberResultFail<LogEvent<R, D>>>()
            .map { BlockError(it.blockNumber, groupId, it.errorMessage) }.groupBy { it.blockNumber }

        val records = logEvents.filterIsInstance<SubscriberResultOk<LogEvent<R, D>>>().map { it.result }

        val saved = logMonitor.onSaveLogs {
            insertOrUpdateRecords(records)
        }

        val stats = gatherStats(records)
        val result = events.map {
            BlockEventResult(
                blockNumber = it.number,
                errors = failed[it.number] ?: emptyList(),
                stats = stats[it.number] ?: BlockStats.empty()
            )
        }

        return BlockListenerResult(result) {
            if (logRecordEventPublisher.isEnabled()) {
                logMonitor.onPublishLogs {
                    sortAndPublishEvents(events, saved)
                }
            }
        }
    }

    private fun gatherStats(logs: List<LogEvent<R, D>>): Map<Long, BlockStats> {
        val start = System.currentTimeMillis()
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
        val stop = System.currentTimeMillis() - start
        logger.info("Gathered stats for {} {} log events ({}ms)", groupId, logs.size, stop)
        return result
    }

    private suspend fun sortAndPublishEvents(events: List<BlockEvent<BB>>, logEvents: List<LogEvent<R, D>>) {
        val start = System.currentTimeMillis()
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
        logger.info("Sorted {} log events for publish ({}ms)", logEvents.size, System.currentTimeMillis() - start)
        for (blockEvent in events) {
            val eventsToInsert = blockEventsToInsert[blockEvent] ?: continue
            val eventsToUpdate = blockEventsToUpdate.getValue(blockEvent)

            val statForUpdated = System.currentTimeMillis()
            logRecordEventPublisher.publish(groupId, addOutMark(eventsToUpdate))
            val stopForUpdated = System.currentTimeMillis() - statForUpdated

            val statForInserted = System.currentTimeMillis()
            logRecordEventPublisher.publish(groupId, addOutMark(eventsToInsert))
            val stopForInserted = System.currentTimeMillis() - statForInserted

            logging(
                message = "published ${eventsToInsert.size} new and ${eventsToUpdate.size}" +
                    " reverted log records (new=${stopForInserted}ms, reverted=${stopForUpdated}ms)",
                event = blockEvent
            )
        }
    }

    private fun addOutMark(records: List<LogRecordEvent>): List<LogRecordEvent> {
        return records.map { it.copy(eventTimeMarks = it.eventTimeMarks.addScannerOut()) }
    }

    private suspend fun prepareBlockEventsBatch(
        batch: List<BlockEvent<BB>>,
        mode: ScanMode
    ): List<SubscriberResult<LogEvent<R, D>>> {
        return coroutineScope {
            subscribers.map { subscriber ->
                asyncWithTraceId(context = NonCancellable) {
                    @Suppress("UNCHECKED_CAST")
                    when (batch[0]) {
                        is NewStableBlockEvent -> onBlock(subscriber, batch as List<NewBlockEvent<BB>>, true, mode)
                        is NewUnstableBlockEvent -> onBlock(subscriber, batch as List<NewBlockEvent<BB>>, false, mode)
                        is RevertedBlockEvent -> onRevertedBlocks(subscriber, batch as List<RevertedBlockEvent<BB>>)
                    }
                }
            }.awaitAll().flatten()
        }
    }

    // Important! If data not saved it means ID of same events in reindex case will be different
    private suspend fun insertOrUpdateRecords(logEvents: List<LogEvent<R, D>>) = coroutineScope {
        val updated: List<LogEvent<R, D>> = logEvents.mapNotNull {
            if (it.logRecordsToUpdate.isEmpty()) {
                return@mapNotNull null
            }
            if (!it.descriptor.shouldSaveLogs()) {
                logging("skipping update for ${it.logRecordsToUpdate.size} log records (disabled)", it.blockEvent)
                return@mapNotNull CompletableDeferred(it)
            }
            asyncWithTraceId(context = NonCancellable) {
                logging("updating ${it.logRecordsToUpdate.size} log records", it.blockEvent)
                withExceptionLogging("Update log records for ${it.blockEvent} by ${it.descriptor.groupId}") {
                    val saved = logService.save(it.descriptor, it.logRecordsToUpdate, it.blockEvent.hash)
                    it.copy(logRecordsToUpdate = saved)
                }
            }
        }.awaitAll()

        val inserted: List<LogEvent<R, D>> = logEvents.mapNotNull {
            if (it.logRecordsToInsert.isEmpty()) {
                return@mapNotNull null
            }
            if (!it.descriptor.shouldSaveLogs()) {
                logging("skipping insert for ${it.logRecordsToInsert.size} log records (disabled)", it.blockEvent)
                return@mapNotNull CompletableDeferred(it)
            }
            asyncWithTraceId(context = NonCancellable) {
                logging("inserting ${it.logRecordsToInsert.size} log records", it.blockEvent)
                val saved = withExceptionLogging("Insert log records for ${it.blockEvent} by $groupId") {
                    logService.save(it.descriptor, it.logRecordsToInsert, it.blockEvent.hash)
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
        stable: Boolean,
        mode: ScanMode,
    ): List<SubscriberResult<LogEvent<R, D>>> {
        val descriptor = subscriber.getDescriptor()
        val blockLogs = blockchainClient.getBlockLogs(
            descriptor = descriptor,
            blocks = events.map { it.block },
            stable = stable,
            mode = mode
        ).toList().associateBy { it.block.number }

        return coroutineScope {
            // Preserve the order of events.
            events.mapNotNull { event ->
                val fullBlock = blockLogs[event.number] ?: return@mapNotNull null
                val task = asyncWithTraceId(context = NonCancellable) {
                    try {
                        val toInsert = prepareLogsToInsert(subscriber, fullBlock, event)
                        val toRevert = if (!stable) {
                            withExceptionLogging("Prepare log records to revert for $event by ${descriptor.id}") {
                                logService.prepareLogsToRevertOnNewBlock(descriptor, fullBlock)
                            }
                        } else {
                            emptyList()
                        }
                        logging(
                            message = "prepared ${toInsert.size} records to insert and ${toRevert.size} records to update",
                            event = event,
                            subscriber = subscriber
                        )
                        val logEvent = LogEvent(
                            blockEvent = event,
                            descriptor = subscriber.getDescriptor(),
                            logRecordsToInsert = toInsert,
                            logRecordsToUpdate = toRevert
                        )
                        SubscriberResultOk(fullBlock.block.number, descriptor.id, logEvent)
                    } catch (e: Exception) {
                        logger.error("Failed to handle block {} by descriptor {}: ", event.number, descriptor.id, e)
                        SubscriberResultFail(event.number, descriptor.id, e.message ?: "Unknown error")
                    }
                }
                task
            }.awaitAll()
        }
    }

    private suspend fun prepareLogsToInsert(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        fullBlock: FullBlock<BB, BL>,
        event: NewBlockEvent<BB>
    ): List<R> {
        if (fullBlock.logs.isEmpty()) {
            logging("no logs in the block", event, subscriber)
            return emptyList()
        }

        val descriptor = subscriber.getDescriptor()
        val logRecords = withExceptionLogging("Prepare log records to insert for $event by ${descriptor.id}") {
            val records = fullBlock.logs.flatMap {
                subscriber.getEventRecords(fullBlock.block, it)
            }
            subscriber.postProcess(event = event, block = fullBlock, logs = records)
        }

        logging("prepared ${logRecords.size} log records to insert", event, subscriber)
        return logRecords
    }

    private suspend fun onRevertedBlocks(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        events: List<RevertedBlockEvent<BB>>
    ): List<SubscriberResult<LogEvent<R, D>>> {
        val descriptor = subscriber.getDescriptor()
        return events.map { event ->

            val toRevertLogs = logService.prepareLogsToRevertOnRevertedBlock(descriptor, event.hash)
            logging("prepared ${toRevertLogs.size} logs to revert", event, subscriber)

            val logEvent = LogEvent(
                blockEvent = event,
                descriptor = descriptor,
                logRecordsToInsert = emptyList(),
                logRecordsToUpdate = toRevertLogs
            )
            SubscriberResultOk(event.number, descriptor.id, logEvent)
        }
    }

    private fun logging(
        message: String,
        event: BlockEvent<*>,
        subscriber: LogEventSubscriber<*, *, *, *>? = null
    ) {
        val id = subscriber?.getDescriptor()?.id ?: ("group $groupId")
        logger.info("Logs for $event by '$id': $message")
    }
}
