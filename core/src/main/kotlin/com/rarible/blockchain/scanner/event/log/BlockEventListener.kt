package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.util.BlockBatcher
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory

class BlockEventListener<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    private val logService: LogService<L, R, D>,
    private val logEventComparator: LogEventComparator<L, R>,
    private val logRecordEventPublisher: LogRecordEventPublisher
) : BlockListener {

    private val logger = LoggerFactory.getLogger(BlockListener::class.java)

    private val subscribers = subscribers.map { BlockEventSubscriber(blockchainClient, it, logService) }
        .also { logger.info("Injected subscribers: {}", it) }

    override suspend fun onBlockEvents(events: List<BlockEvent>) {
        logger.info("Received BlockEvents: {}", events)
        val logEvents = withSpan("onBlockEvents") {
            prepareBlockEvents(events)
        }
        val blockLogsToInsert = hashMapOf<BlockEvent, MutableMap<String, MutableList<R>>>()
        val blockLogsToRemove = hashMapOf<BlockEvent, MutableMap<String, MutableList<R>>>()
        for (logEvent in logEvents) {
            blockLogsToInsert.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.id) { arrayListOf() } += logEvent.logRecordsToInsert
            blockLogsToRemove.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.id) { arrayListOf() } += logEvent.logRecordsToRemove
        }
        for (blockEvent in events) {
            val toInsertGroupIdMap = blockLogsToInsert[blockEvent] ?: continue
            val toRemoveGroupIdMap = blockLogsToRemove.getValue(blockEvent)
            for ((groupId, recordsToRemove) in toRemoveGroupIdMap) {
                logger.info("Publishing {} log records to remove for {} of {}", recordsToRemove.size, groupId, blockEvent)
                if (recordsToRemove.isNotEmpty()) {
                    val logRecordEvents = recordsToRemove.sortedWith(logEventComparator)
                        .map { LogRecordEvent(it, blockEvent.source, true) }
                    logRecordEventPublisher.publish(groupId, logRecordEvents)
                }
            }
            for ((groupId, recordsToInsert) in toInsertGroupIdMap) {
                logger.info("Publishing {} log records to insert for {} of {}", recordsToInsert.size, groupId, blockEvent)
                if (recordsToInsert.isNotEmpty()) {
                    logRecordEventPublisher.publish(
                        groupId,
                        recordsToInsert.sortedWith(logEventComparator)
                            .map { LogRecordEvent(it, blockEvent.source, false) }
                    )
                }
            }
            logger.info("Sent events for {}", blockEvent)
        }
        insertOrRemoveRecords(logEvents)
    }

    suspend fun prepareBlockEvents(events: List<BlockEvent>): List<LogEvent<L, R, D>> {
        val batches = BlockBatcher.toBatches(events)
        return batches.flatMap { prepareBlockEventsBatch(it) }
    }

    private suspend fun prepareBlockEventsBatch(batch: List<BlockEvent>): List<LogEvent<L, R, D>> =
        coroutineScope {
            logger.info("Processing {} subscribers with BlockEvent batch: {}", subscribers.size, batch)
            subscribers.map { subscriber ->
                async {
                    withSpan(
                        name = "processSubscriber",
                        labels = listOf("subscriber" to subscriber.subscriber.javaClass.name)
                    ) {
                        @Suppress("UNCHECKED_CAST")
                        when (batch[0]) {
                            is NewBlockEvent -> subscriber.onNewBlockEvents(batch as List<NewBlockEvent>)
                            is RevertedBlockEvent -> subscriber.onRevertedBlockEvents(batch as List<RevertedBlockEvent>)
                            is ReindexBlockEvent -> subscriber.onReindexBlockEvents(batch as List<ReindexBlockEvent>)
                        }
                    }
                }
            }.awaitAll().flatten()
        }

    private suspend fun insertOrRemoveRecords(logEvents: List<LogEvent<L, R, D>>) = coroutineScope {
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
}
