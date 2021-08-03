package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class LogEventHandler<BB : BlockchainBlock, BL : BlockchainLog, L : Log, D : Descriptor>(
    val subscriber: LogEventSubscriber<BB, BL, D>,
    private val logMapper: LogMapper<BB, BL, L>,
    private val logService: LogService<L, D>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)
    private val descriptor: D = subscriber.getDescriptor()

    init {
        logger.info(
            "Creating LogEventProcessor for ${subscriber.javaClass.simpleName}"
        )
    }

    fun beforeHandleBlock(event: BlockEvent): Flow<L> {

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
                logService.findAndRevert(descriptor, event.reverted.hash)
            ).flattenConcat()
        }
    }


    fun handleLogs(fullBlock: FullBlock<BB, BL>): Flow<L> {
        if (fullBlock.logs.isEmpty()) {
            return emptyFlow()
        }
        logger.info("Handling {} Logs from block: [{}]", fullBlock.logs.size, fullBlock.block.meta)
        val processedLogs = fullBlock.logs.withIndex().asFlow().flatMapConcat { (idx, log) ->
            onLog(fullBlock.block, idx, log)
        }

        return processedLogs
    }

    private fun onLog(block: BB, index: Int, log: BL): Flow<L> = flatten {
        logger.info("Handling single Log: [{}]", log)

        val logs = subscriber.getEventData(block, log)
            .withIndex()
            .map { indexed ->
                val data = indexed.value
                val minorLogIndex = indexed.index
                logMapper.map(
                    block,
                    log,
                    index,
                    minorLogIndex,
                    data,
                    subscriber.getDescriptor()
                )
            }

        saveProcessedLogs(logs)
    }

    private fun saveProcessedLogs(logs: Flow<L>): Flow<L> {
        return logs.map {
            optimisticLock(3) {
                logger.info("Saving Log [{}] for descriptor [{}]", it, descriptor)
                logService.saveOrUpdate(descriptor, it)
            }
        }
    }
}