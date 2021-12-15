package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.util.logTime
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventListener<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logService: LogService<L, R, D>,
    private val groupId: String,
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
        val logFlow = processBlocks(events)
        logFlow.onEach {
            publishLogEvents(it)
            logger.info("BlockEvent [{}] handled", it.blockEvent)
        }.collect()
    }

    private suspend fun processBlocks(events: List<BlockEvent>): Flow<LogEvent<L, R>> {
        return withSpan("process") {
            val logs = logTime("BlockEventListener::processBlockEvents") {
                blockEventProcessor.onBlockEvents(events)
            }
            logs.map {
                val sortedEvents = it.second
                sortedEvents.sortWith(logEventComparator)
                LogEvent(it.first, groupId, sortedEvents)
            }
        }
    }

    private suspend fun publishLogEvents(event: LogEvent<L, R>) {
        logger.info("Publishing {} LogEvents for Block [{}]", event.logRecords.size, event.blockEvent)
        return withSpan("onBlockProcessed") {
            logEventPublisher.publish(event)
        }
    }
}
