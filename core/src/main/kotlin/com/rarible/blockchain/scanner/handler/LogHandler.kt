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
        logger.info("Processing block events ({}): {}", events.size, events)
        val logEvents = withSpan("prepareBlockEventsBatch") {
            val batches = BlockRanges.toBatches(events)
            batches.flatMap { prepareBlockEventsBatch(it) }
        }
        val blockLogsToInsert = hashMapOf<BlockEvent<*>, MutableMap<String, MutableList<R>>>()
        val blockLogsToUpdate = hashMapOf<BlockEvent<*>, MutableMap<String, MutableList<R>>>()
        for (logEvent in logEvents) {
            blockLogsToInsert.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.groupId) { arrayListOf() } += logEvent.logRecordsToInsert
            blockLogsToUpdate.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.groupId) { arrayListOf() } += logEvent.logRecordsToUpdate
        }
        withSpan(name = "publishEvents") {
            for (blockEvent in events) {
                val toInsertGroupIdMap = blockLogsToInsert[blockEvent] ?: continue
                val toUpdateGroupIdMap = blockLogsToUpdate.getValue(blockEvent)
                publishRecordsToUpdate(blockEvent, toUpdateGroupIdMap)
                publishRecordsToInsert(blockEvent, toInsertGroupIdMap)
                logger.info("Sent events for {}", blockEvent)
            }
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
    }

    private suspend fun publishRecordsToInsert(
        blockEvent: BlockEvent<BB>,
        toInsertGroupIdMap: MutableMap<String, MutableList<R>>
    ) {
        for ((groupId, recordsToInsert) in toInsertGroupIdMap) {
            logger.info(
                "Publishing {} new log records for {} of {}",
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

    private suspend fun publishRecordsToUpdate(
        blockEvent: BlockEvent<BB>,
        toUpdateGroupIdMap: MutableMap<String, MutableList<R>>
    ) {
        for ((groupId, recordsToUpdate) in toUpdateGroupIdMap) {
            logger.info(
                "Publishing {} reverted log records for {} of {}",
                recordsToUpdate.size,
                groupId,
                blockEvent
            )
            if (recordsToUpdate.isNotEmpty()) {
                val logRecordEvents = recordsToUpdate.sortedWith(logRecordComparator.reversed())
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

    private suspend fun insertOrUpdateRecords(logEvents: List<LogEvent<R, D>>) = coroutineScope {
        logEvents.map { async { logService.delete(it.descriptor, it.logRecordsToUpdate) } }.awaitAll()
        logEvents.map { async { logService.save(it.descriptor, it.logRecordsToInsert) } }.awaitAll()
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
                val logRecordsToInsert = prepareLogsToInsert(subscriber, fullBlock)
                val logRecordsToRevert = if (!stable)
                    logService.prepareLogsToRevertOnNewBlock(subscriber.getDescriptor(), fullBlock)
                else
                    emptyList()
                logger.info(
                    "Prepared logs for '{}': {} new logs and {} logs to update",
                    subscriber.getDescriptor().id,
                    logRecordsToInsert.size,
                    logRecordsToRevert.size,
                    event
                )
                LogEvent(
                    blockEvent = event,
                    descriptor = subscriber.getDescriptor(),
                    logRecordsToInsert = logRecordsToInsert,
                    logRecordsToUpdate = logRecordsToRevert
                )
            }
        }
    }

    private suspend fun prepareLogsToInsert(
        subscriber: LogEventSubscriber<BB, BL, R, D>,
        fullBlock: FullBlock<BB, BL>
    ): List<R> {
        if (fullBlock.logs.isEmpty()) {
            logger.info(
                "Subscriber '{}': no logs in the block [{}:{}]",
                subscriber.getDescriptor().id,
                fullBlock.block.number,
                fullBlock.block.hash,
            )
            return emptyList()
        }
        logger.info(
            "Subscriber '{}': preparing log records to insert for [{}:{}]",
            subscriber.getDescriptor().id,
            fullBlock.block.number,
            fullBlock.block.hash
        )
        return try {
            fullBlock.logs.flatMap { subscriber.getEventRecords(fullBlock.block, it) }
        } catch (e: Exception) {
            throw BlockHandlerException(
                "Subscriber '${subscriber.getDescriptor().id}': " +
                        "failed to prepare log records for [${fullBlock.block.number}:${fullBlock.block.hash}]",
                e
            )
        }
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
            "Subscriber '{}': prepared {} logs to revert for {}",
            subscriber.getDescriptor().id,
            toRevertLogs.size,
            event
        )
        LogEvent(
            blockEvent = event,
            descriptor = subscriber.getDescriptor(),
            logRecordsToInsert = emptyList(),
            logRecordsToUpdate = toRevertLogs
        )
    }

}
