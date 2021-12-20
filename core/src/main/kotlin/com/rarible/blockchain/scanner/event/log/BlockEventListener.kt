package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventListener<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logService: LogService<L, R, D>,
    private val logEventComparator: LogEventComparator<L, R>,
    private val logEventPublisher: LogEventPublisher
) : BlockListener {

    private val logger = LoggerFactory.getLogger(BlockListener::class.java)

    private val blockEventProcessor: BlockEventProcessor<BB, BL, L, R, D> = BlockEventProcessor(
        blockchainClient,
        subscribers,
        logService
    )

    override suspend fun onBlockEvents(events: List<BlockEvent>) {
        logger.info("Received BlockEvents: {}", events)
        val logEvents = withSpan("onBlockEvents") {
            blockEventProcessor.prepareBlockEvents(events)
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
            for ((groupId, recordsToInsert) in toInsertGroupIdMap) {
                val toRemoveSorted = toRemoveGroupIdMap.getValue(groupId).sortedWith(logEventComparator)
                logger.info("Publishing {} log records to remove for {} of {}", toRemoveSorted.size, groupId, blockEvent)
                if (toRemoveSorted.isNotEmpty()) {
                    logEventPublisher.publish(groupId, blockEvent.source, toRemoveSorted)
                }

                val toInsertSorted = recordsToInsert.sortedWith(logEventComparator)
                logger.info("Publishing {} log records to insert for {} of {}", toInsertSorted.size, groupId, blockEvent)
                if (toInsertSorted.isNotEmpty()) {
                    logEventPublisher.publish(groupId, blockEvent.source, toInsertSorted)
                }
            }
            logger.info("Sent events for {}", blockEvent)
        }
        blockEventProcessor.insertOrRemoveRecords(logEvents)
    }
}
