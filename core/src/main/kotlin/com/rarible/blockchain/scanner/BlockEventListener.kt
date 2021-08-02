package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.logging.RaribleMDCContext
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.slf4j.MDC

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventListener<BB : BlockchainBlock, BL : BlockchainLog, B : Block, L : Log>(
    blockchainClient: BlockchainClient<BB, BL>,
    subscribers: List<LogEventSubscriber<BB, BL>>,
    private val blockService: BlockService<B>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L>,
    logEventListeners: List<LogEventListener<L>>,
    pendingLogService: PendingLogService<BB, L>,
    private val logEventPostProcessors: List<LogEventPostProcessor<L>>,
    private val properties: BlockchainScannerProperties
) : BlockListener {

    private val blockEventHandler: BlockEventHandler<BB, BL, L> = BlockEventHandler(
        blockchainClient,
        subscribers,
        logMapper,
        logEventListeners,
        logService,
        pendingLogService
    )

    override suspend fun onBlockEvent(event: BlockEvent) {
        logger.info("Received BlockEvent [{}]", event)
        event.contextParams.forEach { (key, value) -> MDC.put(key, value) }

        withContext(RaribleMDCContext()) {
            val logs = processBlock(event)
            val status = postProcessBlock(event, logs)
            updateBlockStatus(event, status)
        }
    }

    private suspend fun processBlock(event: BlockEvent): List<L> {
        val logs = blockEventHandler.onBlockEvent(event)
            .toCollection(mutableListOf())
        logger.info("BlockEvent [{}] processed, {} Logs gathered", event, logs.size)
        return logs
    }

    private suspend fun postProcessBlock(event: BlockEvent, logs: List<L>): Block.Status {
        val status = try {
            logger.debug(
                "Starting to post-process {} Logs of BlockEvent [{}] for {} post-processors",
                // TODO оставить только эти слушатели
                logs.size, event, logEventPostProcessors.size
            )

            withTimeout(properties.maxProcessTime) { postProcessLogs(logs) }
            Block.Status.SUCCESS
        } catch (ex: Throwable) {
            logger.error("Unable to handle event [$event]", ex)
            Block.Status.ERROR
        }

        logger.info("Finished BlockEvent [{}] processing, block status is: {}", event, status)
        return status
    }

    private suspend fun postProcessLogs(logs: List<L>) {
        return (logEventPostProcessors).asFlow().map {
            try {
                it.postProcessLogs(logs)
            } catch (th: Throwable) {
                throw Exception("Logs post-processing failed in ${it.javaClass.name}", th)
            }
        }.collect()
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