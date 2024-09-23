package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
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

class ReconciliationLogHandlerImpl<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val logService: LogService<R, D>,
    private val reconciliationProperties: ReconciliationProperties,
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logHandlerFactory: LogHandlerFactory<BB, BL, R, D>,
    private val reindexer: BlockReindexer<BB, BL, R, D>,
    private val planner: BlockScanPlanner<BB>,
    private val monitor: LogReconciliationMonitor,
    subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
) : ReconciliationLogHandler {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)
    private val logServiceWrapper = LogServiceWrapper(logService)

    private val subscribersByCollection = subscribers.groupBy {
        it.getDescriptor().collection
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
        subscribersByCollection.map { entity ->
            asyncWithTraceId(context = NonCancellable) { checkCollectionLogs(blockNumber, entity.key, entity.value) }
        }.awaitAll()
    }

    private suspend fun checkCollectionLogs(
        blockNumber: Long,
        collection: String,
        subscribers: List<LogEventSubscriber<BB, BL, R, D>>
    ) {
        val savedLogCount = getSavedLogCount(blockNumber, collection)
        val chainLogCount = getChainLogCount(blockNumber, subscribers)
        if (savedLogCount != chainLogCount) {
            monitor.onInconsistency()
            logger.error(
                "Saved logs count for block {} and collection '{}' are not consistent (saved={}, fetched={})",
                blockNumber, collection, savedLogCount, chainLogCount
            )
            if (reconciliationProperties.autoReindex) {
                reindex(blockNumber)
            }
        }
    }

    private suspend fun getSavedLogCount(blockNumber: Long, collection: String): Long {
        return logService.countByBlockNumber(collection, blockNumber)
    }

    private suspend fun getChainLogCount(
        blockNumber: Long,
        subscribers: List<LogEventSubscriber<BB, BL, R, D>>
    ) = coroutineScope {
        val block = blockchainClient.getBlock(blockNumber) ?: error("Can't get stable block $blockNumber")
        val events = listOf(NewStableBlockEvent(block, ScanMode.REINDEX))

        subscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                asyncWithTraceId(context = NonCancellable) {
                    logHandlerFactory.create(
                        groupId = groupId,
                        subscribers = subscribers,
                        logService = logServiceWrapper,
                        logRecordEventPublisher = NoOpRecordEventPublisher,
                    ).process(events)
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

    private class LogServiceWrapper<R : LogRecord, D : Descriptor>(
        private val delegate: LogService<R, D>,
    ) : LogService<R, D> {
        override suspend fun delete(descriptor: D, record: R): R {
            throw IllegalStateException("Should not be called")
        }

        override suspend fun save(descriptor: D, records: List<R>, blockHash: String): List<R> = records

        override suspend fun prepareLogsToRevertOnNewBlock(descriptor: D, fullBlock: FullBlock<*, *>): List<R> {
            return delegate.prepareLogsToRevertOnNewBlock(descriptor, fullBlock)
        }

        override suspend fun prepareLogsToRevertOnRevertedBlock(descriptor: D, revertedBlockHash: String): List<R> {
            throw IllegalStateException("Should not be called")
        }

        override suspend fun countByBlockNumber(collection: String, blockNumber: Long): Long {
            return delegate.countByBlockNumber(collection, blockNumber)
        }
    }
}

object NoOpRecordEventPublisher : LogRecordEventPublisher {
    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {}
}
