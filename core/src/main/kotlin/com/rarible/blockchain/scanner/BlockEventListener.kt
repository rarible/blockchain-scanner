package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.logging.RaribleMDCContext
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.slf4j.MDC

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventListener<BB : BlockchainBlock, BL : BlockchainLog, B : Block, L : Log, R : LogRecord<L>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    private val blockService: BlockService<B>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    pendingLogService: PendingLogService<BB, L, R, D>,
    private val logEventPublisher: LogEventPublisher<L>
) : BlockListener {

    private val blockEventHandler: BlockEventHandler<BB, BL, L, R, D> = BlockEventHandler(
        blockchainClient,
        subscribers,
        logMapper,
        logService,
        pendingLogService
    )

    override suspend fun onBlockEvent(event: BlockEvent) {
        logger.info("Received BlockEvent [{}]", event)
        event.contextParams.forEach { (key, value) -> MDC.put(key, value) }

        withContext(RaribleMDCContext()) {
            val logs = processBlock(event)
            val status = logEventPublisher.onBlockProcessed(event, logs)
            updateBlockStatus(event, status)
        }
    }

    private suspend fun processBlock(event: BlockEvent): List<LogRecord<L>> {
        val logs = blockEventHandler.onBlockEvent(event).toList()
        logger.info("BlockEvent [{}] processed, {} Logs gathered", event, logs.size)
        return logs
    }

    private suspend fun updateBlockStatus(event: BlockEvent, status: Block.Status) {
        try {
            blockService.updateBlockStatus(event.block.number, status)
        } catch (ex: Throwable) {
            logger.error("Unable to save Block from BlockEvent [{}] with status {}", event, status, ex)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BlockListener::class.java)
    }
}