package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.reindex.BlockReindexer
import com.rarible.blockchain.scanner.reindex.BlockScanPlanner
import com.rarible.blockchain.scanner.reindex.LogHandlerFactory
import com.rarible.blockchain.scanner.util.toRanges
import com.rarible.core.common.asyncWithTraceId
import com.rarible.core.common.mapAsync
import com.rarible.core.logging.withTraceId
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ReconciliationLogHandlerImpl<
    BB : BlockchainBlock,
    BL : BlockchainLog,
    R : LogRecord,
    D : Descriptor<S>,
    S : LogStorage
    >(
    private val reconciliationProperties: ReconciliationProperties,
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logHandlerFactory: LogHandlerFactory<BB, BL, R, D, S>,
    private val planner: BlockScanPlanner<BB>,
    private val reindexer: BlockReindexer<BB, BL, R, D, S>,
    private val publisher: LogRecordEventPublisher,
    private val monitor: LogReconciliationMonitor,
    subscribers: List<LogEventSubscriber<BB, BL, R, D, S>>,
) : ReconciliationLogHandler {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val logHandlersByStorageAndGroupId: Map<S, List<LogHandler<BB, BL, R, D, S>>> =
        subscribers.groupBy {
            it.getDescriptor().storage
        }.mapValues { (_, subscribersByStorage) ->
            subscribersByStorage
                .groupBy { it.getDescriptor().groupId }
                .map { (groupId, subscribers) ->
                    logHandlerFactory.create(
                        groupId = groupId,
                        subscribers = subscribers,
                        logRecordEventPublisher = NoOpRecordEventPublisher,
                        readOnly = true,
                    )
                }
        }

    override suspend fun handle(blockRange: TypedBlockRange, batchSize: Int) {
        coroutineScope {
            val reconciledBlockRangesFlow = blockRange.range.asFlow()
                .withTraceId()
                .mapAsync(batchSize) { blockNumber ->
                    withContext(context = NonCancellable + MDCContext()) {
                        if (isBlockConsistent(blockNumber)) null else blockNumber
                    }
                }.filterNotNull()
                .toRanges()

            if (reconciliationProperties.autoReindex.enabled) {
                reindex(reconciledBlockRangesFlow, reconciliationProperties.autoReindex.publishEvents)
            } else {
                reconciledBlockRangesFlow.collect()
            }
            logger.info("Handled {}", blockRange)
        }
    }

    private suspend fun isBlockConsistent(blockNumber: Long): Boolean {
        return coroutineScope {
            logHandlersByStorageAndGroupId.map { (storage, logHandlers) ->
                asyncWithTraceId(context = NonCancellable) {
                    areBlockLogsConsistent(blockNumber, storage, logHandlers)
                }
            }.awaitAll().all { it }
        }
    }

    private suspend fun areBlockLogsConsistent(
        blockNumber: Long,
        storage: S,
        logHandlers: List<LogHandler<BB, BL, R, D, S>>
    ): Boolean {
        val savedLogCount = getSavedLogCount(blockNumber, storage)
        val chainLogCount = getChainLogCount(blockNumber, logHandlers)
        if (savedLogCount == chainLogCount) {
            return true
        }
        monitor.onInconsistency()
        logger.error(
            "Saved logs count for block {} and log storage '{}' are not consistent (saved={}, fetched={})",
            blockNumber, storage::class.simpleName, savedLogCount, chainLogCount
        )
        return false
    }

    private suspend fun getSavedLogCount(blockNumber: Long, storage: S): Long {
        return storage.countByBlockNumber(blockNumber)
    }

    private suspend fun getChainLogCount(
        blockNumber: Long,
        logHandlers: List<LogHandler<BB, BL, R, D, S>>
    ) = coroutineScope {
        val block = blockchainClient.getBlock(blockNumber) ?: error("Can't get stable block $blockNumber")
        val events = listOf(NewStableBlockEvent(block, ScanMode.REINDEX))

        logHandlers
            .map { logHandler ->
                asyncWithTraceId(context = NonCancellable) {
                    logHandler.process(events)
                }
            }
            .awaitAll()
            .map { blockListenerResult ->
                asyncWithTraceId(context = NonCancellable) {
                    blockListenerResult.publish()
                    blockListenerResult.blocks
                }
            }.awaitAll().flatten()
            .sumOf { it.stats.inserted }
            .toLong()
    }

    private suspend fun reindex(blockNumberRanges: Flow<LongRange>, publishEvents: Boolean) {
        val reindexFlow = blockNumberRanges
            .mapAsync(reconciliationProperties.reindexParallelism) { blockNumberRange ->
                val plan = planner.getPlan(
                    BlockRange(
                        from = blockNumberRange.first,
                        to = blockNumberRange.last,
                        batchSize = reconciliationProperties.reindexBatchSize,
                    )
                )
                reindexer.reindex(
                    baseBlock = plan.baseBlock,
                    blocksRanges = plan.ranges,
                    publisher = publisher.takeIf { publishEvents },
                ).collect {
                    logger.info("block {} was re-indexed", it)
                }
                blockNumberRange
            }
        val nReindexedBlocks = reindexFlow.fold(0L) { sum, r2 -> sum + r2.span }
        logger.info("Reindexed {} blocks", nReindexedBlocks)
    }
}

object NoOpRecordEventPublisher : LogRecordEventPublisher {
    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {}
}

private val LongRange.span get() = last - first + 1
