package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
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
        logger.info(
            "Creating LogEventProcessor for ${subscriber.javaClass.simpleName}"
        )
    }

    fun beforeHandleBlock(event: BlockEvent): Flow<R> {

        logger.info(
            "Deleting all reverted logs before handling block event [{}] by descriptor: [{}]",
            event, descriptor
        )

        val deletedAndReverted = logService.findAndDelete(descriptor, event.block.hash, Log.Status.REVERTED)
            .dropWhile { true }

        return if (event.reverted == null) {
            deletedAndReverted
        } else {
            logger.info("BlockEvent has reverted Block: [{}], reverting it in indexer", event.reverted)
            flowOf(
                deletedAndReverted,
                logService.findAndRevert(descriptor, event.reverted!!.hash)
            ).flattenConcat()
        }
    }

    fun handleLogs(fullBlock: FullBlock<BB, BL>): Flow<R> = flow {
        if (fullBlock.logs.isNotEmpty()) {
            logger.info("Handling {} Logs from block: [{}]", fullBlock.logs.size, fullBlock.block.meta)
            val processedLogs = fullBlock.logs.withIndex().asFlow().flatMapConcat { (idx, log) ->
                onLog(fullBlock.block, idx, log)
            }
            emitAll(
                withSpan("saveLogs", "db") {
                    logService.save(descriptor, processedLogs.toList())
                }.asFlow()
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun onLog(block: BB, index: Int, log: BL): Flow<R> = flatten {
        logger.info("Handling single Log: [{}]", log.hash)

        subscriber.getEventRecords(block, log)
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
            }
    }
}
