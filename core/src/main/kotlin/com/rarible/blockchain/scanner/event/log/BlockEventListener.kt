package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.logTime
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventListener<BB : BlockchainBlock, BL : BlockchainLog, B : Block, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    private val blockService: BlockService<B>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    pendingLogService: PendingLogService<L, R, D>,
    private val logEventPublisher: LogEventPublisher<L, R>
) : BlockListener {

    private val logger = LoggerFactory.getLogger(BlockListener::class.java)

    private val blockEventProcessor: BlockEventProcessor<BB, BL, L, R, D> = BlockEventProcessor(
        blockchainClient,
        subscribers,
        logMapper,
        logService,
        pendingLogService
    )

    override suspend fun onBlockEvents(events: List<BlockEvent>) {
        logger.info("Received BlockEvents: {}", events)
        val logFlow = processBlocks(events)
        logFlow.onEach { blockLogs ->
            blockLogs.onEach {
                val event = it.key
                val logEvents = it.value
                val status = publishLogEvents(event, logEvents)
                updateBlockStatus(event, status)
                logger.info("BlockEvent [{}] handled, status updated: {}", event, status)
            }
        }.collect()
    }

    private suspend fun processBlocks(events: List<BlockEvent>): Flow<Map<BlockEvent, List<R>>> {
        return withSpan("process") {
            val logs = logTime("BlockEventListener::processBlockEvents") {
                blockEventProcessor.onBlockEvents(events)
            }
            logs
        }
    }

    private suspend fun publishLogEvents(event: BlockEvent, logs: List<R>): Block.Status {
        return withSpan("onBlockProcessed") {
            logEventPublisher.onBlockProcessed(event, logs)
        }
    }

    private suspend fun updateBlockStatus(event: BlockEvent, status: Block.Status) {
        withSpan("updateBlockStatus", type = "db") {
            try {
                blockService.updateStatus(event.number, status)
            } catch (ex: Throwable) {
                logger.error("Unable to save Block from BlockEvent [{}] with status {}", event, status, ex)
            }
        }
    }

}
