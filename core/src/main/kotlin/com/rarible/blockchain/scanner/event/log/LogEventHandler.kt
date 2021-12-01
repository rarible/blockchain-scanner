package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class LogEventHandler<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    val subscriber: LogEventSubscriber<BB, BL, L, R, D>,
    private val logMapper: LogMapper<BB, BL, L>,
    private val logService: LogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(subscriber.javaClass)
    private val descriptor: D = subscriber.getDescriptor()
    private val name = subscriber.javaClass.simpleName

    init {
        logger.info("Creating LogEventHandler for {}", name)
    }

    suspend fun revert(blockEvent: RevertedBlockEvent): List<R> {
        val reverted = logService.findAndDelete(descriptor, blockEvent.hash).toList()
            .map { it.withLog(it.log!!.withStatus(Log.Status.REVERTED)) as R }
        logger.info("Reverted {} Logs for Block [{}], subscriber {}", reverted.size, blockEvent, name)
        return reverted
    }

    suspend fun handleLogs(fullBlock: FullBlock<BB, BL>): List<R> {
        val block = fullBlock.block
        val logs = fullBlock.logs

        return if (logs.isNotEmpty()) {
            logger.info("Handling {} Logs of Block [{}:{}], subscriber {} ", logs.size, block.number, block.hash, name)
            val processedLogs = logs.withIndex().flatMap { (idx, log) ->
                onLog(block, idx, log)
            }
            withSpan("saveLogs", "db") {
                logService.save(descriptor, processedLogs)
            }
        } else {
            emptyList()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun onLog(block: BB, index: Int, log: BL): List<R> {
        return subscriber.getEventRecords(block, log)
            .withIndex()
            .map { indexed ->
                val record = indexed.value
                val minorLogIndex = indexed.index
                val recordLog = logMapper.map(
                    block,
                    log,
                    index,
                    minorLogIndex,
                    subscriber.getDescriptor()
                )
                record.withLog(recordLog) as R
            }.toList()
    }
}