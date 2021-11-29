package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
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
class LogEventHandler<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    val subscriber: LogEventSubscriber<BB, BL, L, R, D>,
    private val logMapper: LogMapper<BB, BL, L>,
    private val logService: LogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(subscriber.javaClass)
    private val descriptor: D = subscriber.getDescriptor()

    init {
        logger.info("Creating LogEventProcessor for ${subscriber.javaClass.simpleName}")
    }

    suspend fun beforeHandleBlock(event: BlockEvent): List<R> {
        logger.info(
            "Deleting all reverted logs before handling block event [{}] by descriptor: [{}]",
            event, descriptor
        )

        val deleted = logService.findAndDelete(descriptor, event.hash, Log.Status.REVERTED).toList()

        return if (event is NewBlockEvent) {
            deleted
        } else {
            logger.info("BlockEvent has reverted Block: [{}], reverting it in indexer", event.hash)
            val reverted = logService.findAndRevert(descriptor, event.hash).toList()
            deleted + reverted
        }
    }

    suspend fun handleLogs(fullBlock: FullBlock<BB, BL>): List<R> {
        return if (fullBlock.logs.isNotEmpty()) {
            logger.info("Handling {} Logs from block: [{}]", fullBlock.logs.size, fullBlock.block.meta)
            val processedLogs = fullBlock.logs.withIndex().flatMap { (idx, log) ->
                onLog(fullBlock.block, idx, log)
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
        logger.info("Handling single Log: [{}]", log.hash)

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
