package com.rarible.blockchain.scanner.task

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.BlockScanPlanner
import com.rarible.blockchain.scanner.reindex.ReindexParam
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory

// Implement in blockchains if needed
abstract class BlockReindexTaskHandler<
    BB : BlockchainBlock,
    BL : BlockchainLog,
    R : LogRecord,
    TR : TransactionRecord,
    D : Descriptor<S>,
    S : LogStorage,
    P : ReindexParam
    >(
    private val manager: BlockchainScannerManager<BB, BL, R, TR, D, S>,
) : TaskHandler<Long> {

    protected val logger: Logger = LoggerFactory.getLogger(javaClass)

    override val type = ReindexParam.BLOCK_SCANNER_REINDEX_TASK

    private val enabled = manager.properties.task.reindex.enabled
    private val monitor = manager.reindexMonitor

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && enabled
    }

    private val defaultReindexer = manager.blockReindexer
    private val defaultPlanner = manager.blockScanPlanner
    private val publisher = manager.logRecordEventPublisher

    override fun runLongTask(from: Long?, param: String): Flow<Long> = flow {
        val taskParamParsed = getParam(param)
        val taskParam = if (taskParamParsed.range.to == null) {
            val lastBlock = manager.blockService.getLastBlock()?.id
            logger.warn("Range.to is empty, will be used the last block: $lastBlock")
            taskParamParsed.copyWithRange(range = taskParamParsed.range.copy(to = lastBlock))
        } else {
            taskParamParsed
        }
        val filter = getFilter(taskParam)
        val reindexer = getBlockReindexer(taskParam, defaultReindexer)
        val planner = getBlockScanPlanner(taskParam, defaultPlanner)
        val publisher = if (taskParam.publishEvents) publisher else null
        val (reindexRanges, baseBlock, planFrom, planTo) = planner.getPlan(taskParam.range, from)
        val blocks = reindexer.reindex(
            baseBlock,
            reindexRanges,
            filter,
            publisher
        )
        emitAll(blocks.map { block ->
            ReindexResult(block.id, taskParam, planFrom, planTo)
        })
    }.map {
        logger.info("Re-index finished up to block ${it.blockId}")
        monitor.onReindex(
            name = it.taskParam.name,
            from = it.taskParam.range.from,
            to = it.taskParam.range.to,
            state = getTaskProgress(it.planFrom, it.planTo, it.blockId)
        )
        it.blockId
    }

    protected open fun getBlockReindexer(
        param: P,
        defaultReindexer: BlockReindexer<BB, BL, R, D, S>
    ): BlockReindexer<BB, BL, R, D, S> {
        return defaultReindexer
    }

    protected open fun getBlockScanPlanner(param: P, defaultPlanner: BlockScanPlanner<BB>): BlockScanPlanner<BB> {
        return defaultPlanner
    }

    private fun getTaskProgress(from: Long, to: Long, position: Long): Double {
        if (position == 0L || from > to) return 0.toDouble()
        return (to.toDouble() - from.toDouble()) / position.toDouble()
    }

    abstract fun getFilter(param: P): SubscriberFilter<BB, BL, R, D, S>

    abstract fun getParam(param: String): P

    data class ReindexResult<P>(
        val blockId: Long,
        val taskParam: P,
        val planFrom: Long,
        val planTo: Long
    )
}
