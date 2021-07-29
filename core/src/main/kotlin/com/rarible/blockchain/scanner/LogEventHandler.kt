package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class LogEventHandler<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    val subscriber: LogEventSubscriber<OL, OB>,
    private val logMapper: LogMapper<OL, OB, L>,
    private val logService: LogService<L>,
    private val logEventListeners: List<LogEventListener<L>>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    init {
        logger.info(
            "Creating LogEventProcessor for ${subscriber.javaClass.simpleName}," +
                    " got onLogEventListeners: ${logEventListeners.joinToString { it.javaClass.simpleName }}"
        )
    }

    fun beforeHandleBlock(event: BlockEvent): Flow<L> {
        val collection = subscriber.getDescriptor().collection
        val topic = subscriber.getDescriptor().topic

        logger.info("Before handling block event [{}] by descriptor: [{}]", event, subscriber.getDescriptor())
        return if (event.reverted != null) {
            logger.info("BlockEvent has reverted Block: [{}], reverting it in indexer", event.reverted)
            logService
                .findAndDelete(collection, event.block.hash, topic, Log.Status.REVERTED)
                .onCompletion { (logService.findAndRevert(collection, topic, event.reverted.hash)) }
        } else {
            logService.findAndDelete(collection, event.block.hash, topic, Log.Status.REVERTED)
                .onCompletion { emptyFlow<L>() }
        }
    }

    fun handleLogs(block: OB, logs: List<OL>): Flow<L> {
        if (logs.isNotEmpty()) {
            logger.info("Handling {} Logs from block: [{}]", logs.size, block.meta)
        }
        val processedLogs = logs.withIndex().asFlow().flatMapConcat { (idx, log) ->
            onLog(block, idx, log)
        }.onEach {
            notifyListeners(it)
        }

        return processedLogs
    }

    private suspend fun onLog(block: OB, index: Int, log: OL): Flow<L> {
        logger.info("Handling single Log: [{}]", log)

        val logs = subscriber.getEventData(log, block).asFlow()
            .toCollection(mutableListOf())
            .mapIndexed { minorLogIndex, data ->
                logMapper.map(
                    block,
                    log,
                    index,
                    minorLogIndex,
                    data,
                    subscriber.getDescriptor()
                )
            }.asFlow()

        return saveProcessedLogs(logs)
    }

    private suspend fun saveProcessedLogs(logs: Flow<L>): Flow<L> {
        val collection = subscriber.getDescriptor().collection
        return logs.map {
            optimisticLock(3) {
                logger.info("Saving Log [{}] to '{}'", it, collection)
                logService.saveOrUpdate(collection, it)
            }
        }
    }

    private suspend fun notifyListeners(logEvent: L) {
        logEventListeners.forEach { it.onLogEvent(logEvent) }
    }

}