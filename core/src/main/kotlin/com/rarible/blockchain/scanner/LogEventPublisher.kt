package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.ProcessedBlockEvent
import com.rarible.blockchain.scanner.util.logTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

@ExperimentalCoroutinesApi
class LogEventPublisher<L : Log, R : LogRecord<L, *>>(
    private val logEventListeners: List<LogEventListener<L, R>>,
    private val retryPolicy: ScanRetryPolicyProperties
) {

    private val logger = LoggerFactory.getLogger(LogEventPublisher::class.java)

    suspend fun onBlockProcessed(event: BlockEvent, logs: List<R>): Block.Status {
        val status = logTime("onBlockProcessed [${event.block.number}]") {
            try {
                logger.debug(
                    "Starting to notify all listeners for {} Logs of BlockEvent [{}] for {} post-processors",
                    logs.size, event, logEventListeners.size
                )

                withTimeout(retryPolicy.maxProcessTime.toMillis()) { onLogsProcessed(event, logs) }
                Block.Status.SUCCESS
            } catch (ex: Throwable) {
                logger.error("Unable to handle event [$event]", ex)
                Block.Status.ERROR
            }
        }

        logger.info("Finished BlockEvent [{}] processing, block status is: {}", event, status)
        return status
    }

    private suspend fun onLogsProcessed(event: BlockEvent, logs: List<R>) {
        return (logEventListeners).asFlow().map {
            try {
                it.onBlockLogsProcessed(ProcessedBlockEvent(event, logs))
            } catch (th: Throwable) {
                throw Exception("Logs post-processing failed in ${it.javaClass.name}", th)
            }
        }.collect()
    }
}
