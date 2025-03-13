package com.rarible.blockchain.scanner.task

import com.rarible.blockchain.scanner.BlockchainScannerManager
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.ReindexParam
import com.rarible.blockchain.scanner.reindex.SubscriberFilter
import com.rarible.core.task.TaskHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
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
    manager: BlockchainScannerManager<BB, BL, R, TR, D, S>
) : TaskHandler<Long> {

    protected val logger: Logger = LoggerFactory.getLogger(javaClass)

    override val type = ReindexParam.BLOCK_SCANNER_REINDEX_TASK

    private val enabled = manager.properties.task.reindex.enabled
    private val monitor = manager.reindexMonitor

    override suspend fun isAbleToRun(param: String): Boolean {
        return param.isNotBlank() && enabled
    }

    private val reindexer = manager.blockReindexer
    private val planner = manager.blockScanPlanner
    private val publisher = manager.logRecordEventPublisher

    override fun runLongTask(from: Long?, param: String): Flow<Long> {
        val taskParam = getParam(param)
        val filter = getFilter(taskParam)
        val publisher = if (taskParam.publishEvents) publisher else null
        val (reindexRanges, baseBlock, planFrom, planTo) = runBlocking {
            planner.getPlan(taskParam.range, from)
        }
        return flow {
            val blocks = reindexer.reindex(
                baseBlock,
                reindexRanges,
                filter,
                publisher
            )
            emitAll(blocks)
        }.map {
            logger.info("Re-index finished up to block $it")
            monitor.onReindex(
                name = taskParam.name,
                from = taskParam.range.from,
                to = taskParam.range.to,
                state = getTaskProgress(planFrom, planTo, it.id)
            )
            it.id
        }
    }

    protected open fun getReindexer(param: P, defaultReindexer: BlockReindexer<BB, BL, R, D, S>): BlockReindexer<BB, BL, R, D, S> {
        return defaultReindexer
    }

    private fun getTaskProgress(from: Long, to: Long, position: Long): Double {
        if (position == 0L || from > to) return 0.toDouble()
        return (to.toDouble() - from.toDouble()) / position.toDouble()
    }

    abstract fun getFilter(param: P): SubscriberFilter<BB, BL, R, D, S>

    abstract fun getParam(param: String): P
}
