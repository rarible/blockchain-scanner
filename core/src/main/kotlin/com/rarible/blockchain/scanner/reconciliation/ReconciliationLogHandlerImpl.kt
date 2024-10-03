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
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
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
    private val reindexer: BlockReindexer<BB, BL, R, D, S>,
    private val planner: BlockScanPlanner<BB>,
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

    override suspend fun check(blocksRange: TypedBlockRange, batchSize: Int): Long = coroutineScope {
        blocksRange.range
            .chunked(batchSize)
            .map { blockNumbers ->
                blockNumbers.map { blockNumber ->
                    asyncWithTraceId(context = NonCancellable) {
                        handleBlock(blockNumber)
                        blockNumber
                    }
                }.awaitAll()
            }.flatten().last()
    }

    private suspend fun handleBlock(blockNumber: Long): List<Unit> = coroutineScope {
        logHandlersByStorageAndGroupId.map { (storage, logHandlers) ->
            asyncWithTraceId(context = NonCancellable) { checkCollectionLogs(blockNumber, storage, logHandlers) }
        }.awaitAll()
    }

    private suspend fun checkCollectionLogs(
        blockNumber: Long,
        storage: S,
        logHandlers: List<LogHandler<BB, BL, R, D, S>>
    ) {
        val savedLogCount = getSavedLogCount(blockNumber, storage)
        val chainLogCount = getChainLogCount(blockNumber, logHandlers)
        if (savedLogCount != chainLogCount) {
            monitor.onInconsistency()
            logger.error(
                "Saved logs count for block {} and log storage '{}' are not consistent (saved={}, fetched={})",
                blockNumber, storage::class.simpleName, savedLogCount, chainLogCount
            )
            if (reconciliationProperties.autoReindex) {
                reindex(blockNumber)
            }
        }
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
            .map {
                asyncWithTraceId(context = NonCancellable) {
                    it.publish()
                    it.blocks
                }
            }.awaitAll().flatten()
            .sumOf { it.stats.inserted }
            .toLong()
    }

    private suspend fun reindex(blockNumber: Long) {
        val blockRange = BlockRange(from = blockNumber, to = null, batchSize = null)
        val (reindexRanges, baseBlock) = planner.getPlan(blockRange)
        reindexer.reindex(
            baseBlock = baseBlock,
            blocksRanges = reindexRanges,
        ).collect {
            logger.info("block $blockNumber was re-indexed")
        }
    }
}

object NoOpRecordEventPublisher : LogRecordEventPublisher {
    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {}
}
