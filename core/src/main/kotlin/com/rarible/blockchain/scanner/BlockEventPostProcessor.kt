package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.ProcessedBlockEvent
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

class BlockEventPostProcessor<L : Log>(
    private val logEventListeners: List<LogEventListener<L>>,
    private val properties: BlockchainScannerProperties
) {

    suspend fun onBlockProcessed(event: BlockEvent, logs: List<L>): Block.Status {
        val status = try {
            logger.debug(
                "Starting to notify all listeners for {} Logs of BlockEvent [{}] for {} post-processors",
                logs.size, event, logEventListeners.size
            )

            withTimeout(properties.maxProcessTime) { onLogsProcessed(event, logs) }
            Block.Status.SUCCESS
        } catch (ex: Throwable) {
            logger.error("Unable to handle event [$event]", ex)
            Block.Status.ERROR
        }

        logger.info("Finished BlockEvent [{}] processing, block status is: {}", event, status)
        return status
    }

    private suspend fun onLogsProcessed(event: BlockEvent, logs: List<L>) {
        return (logEventListeners).asFlow().map {
            try {
                it.onBlockLogsProcessed(ProcessedBlockEvent(event, logs))
            } catch (th: Throwable) {
                throw Exception("Logs post-processing failed in ${it.javaClass.name}", th)
            }
        }.collect()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BlockListener::class.java)
    }

}