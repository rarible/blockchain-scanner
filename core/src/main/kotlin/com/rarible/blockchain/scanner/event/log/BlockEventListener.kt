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
        val logs = hashMapOf<BlockEvent, MutableMap<String, MutableList<R>>>()
        val logEvents = withSpan("onBlockEvents") {
            blockEventProcessor.processBlockEvents(events)
        }
        for (logEvent in logEvents) {
            logs.getOrPut(logEvent.blockEvent) { hashMapOf() }
                .getOrPut(logEvent.descriptor.id) { arrayListOf() } += logEvent.logRecords
        }
        for ((blockEvent, groupIdMap) in logs) {
            for ((groupId, logRecords) in groupIdMap) {
                val sortedRecords = logRecords.sortedWith(logEventComparator)
                logger.info("Publishing {} log records for {} of {}", sortedRecords.size, groupId, blockEvent)
                logEventPublisher.publish(groupId, blockEvent.source, sortedRecords)
            }
            logger.info("Sent events for {}", blockEvent)
        }
    }
}
