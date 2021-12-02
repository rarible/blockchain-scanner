package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.ProcessedBlockEvent
import com.rarible.blockchain.scanner.util.logTime
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

@ExperimentalCoroutinesApi
class LogEventPublisher<L : Log<L>, R : LogRecord<L, *>>(
    private val logEventListeners: List<LogEventListener<L, R>>,
    private val retryPolicy: ScanRetryPolicyProperties
) {

    private val logger = LoggerFactory.getLogger(LogEventPublisher::class.java)

    suspend fun onBlockProcessed(event: BlockEvent, logs: List<R>) {
        val status = logTime("onBlockProcessed [${event.number}]") {
            try {
                logger.debug(
                    "Starting to notify all listeners for {} Logs of BlockEvent [{}] for {} post-processors",
                    logs.size, event, logEventListeners.size
                )

                withTimeout(retryPolicy.maxProcessTime.toMillis()) { onLogsProcessed(event, logs) }
            } catch (ex: Throwable) {
                logger.error("Unable to handle event [$event]", ex)
            }
        }

        logger.info("Finished BlockEvent [{}] processing, block status: {}", event, status)
        return status
    }

    private suspend fun onLogsProcessed(event: BlockEvent, logs: List<R>) {
        logEventListeners.map {
            try {
                it.onBlockLogsProcessed(ProcessedBlockEvent(event, logs))
            } catch (th: Throwable) {
                throw Exception("Logs post-processing failed in ${it.javaClass.name}", th)
            }
        }
    }
}
