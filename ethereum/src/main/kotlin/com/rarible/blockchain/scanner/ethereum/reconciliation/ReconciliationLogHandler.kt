package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.framework.service.LogService
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

// TODO make it generic
class ReconciliationLogHandler(
    private val logRepository: EthereumLogRepository,
    private val logService: EthereumLogService,
    private val onReconciliationListeners: List<OnReconciliationListener>,
    private val scannerProperties: EthereumScannerProperties,
    private val ethereumClient: BlockchainClient<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor>,
    private val logHandlerFactory: LogHandlerFactory<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>,
    private val reindexer: BlockReindexer<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>,
    private val planner: BlockScanPlanner<EthereumBlockchainBlock>,
    private val monitor: EthereumLogReconciliationMonitor,
    subscribers: List<EthereumLogEventSubscriber>,
) {

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val subscribersByCollection = subscribers.groupBy {
        it.getDescriptor().collection
    }

    suspend fun check(blocksRange: TypedBlockRange, batchSize: Int) = coroutineScope {
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

    private suspend fun handleBlock(blockNumber: Long) = coroutineScope {
        subscribersByCollection.map { entity ->
            asyncWithTraceId(context = NonCancellable) { checkCollectionLogs(blockNumber, entity.key, entity.value) }
        }.awaitAll()
    }

    private suspend fun checkCollectionLogs(
        blockNumber: Long,
        collection: String,
        subscribers: List<EthereumLogEventSubscriber>
    ) {
        val savedLogCount = getSavedLogCount(blockNumber, collection)
        val chainLogCount = getChainLogCount(blockNumber, subscribers)
        if (savedLogCount != chainLogCount) {
            monitor.onInconsistency()
            logger.error(
                "Saved logs count for block {} and collection '{}' are not consistent (saved={}, fetched={})",
                blockNumber, collection, savedLogCount, chainLogCount
            )
            if (scannerProperties.reconciliation.autoReindex) {
                reindex(blockNumber)
            }
        }
    }

    private suspend fun getSavedLogCount(blockNumber: Long, collection: String): Long {
        return logRepository.countByBlockNumber(collection, blockNumber)
    }

    private suspend fun getChainLogCount(
        blockNumber: Long,
        subscribers: List<EthereumLogEventSubscriber>
    ) = coroutineScope {
        val block = ethereumClient.getBlock(blockNumber) ?: error("Can't get stable block $blockNumber")
        val events = listOf(NewStableBlockEvent(block, ScanMode.REINDEX))

        subscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                asyncWithTraceId(context = NonCancellable) {
                    logHandlerFactory.create(
                        groupId = groupId,
                        subscribers = subscribers,
                        logService = createLogService(),
                        logRecordEventPublisher = createEmptyLogRecordEventPublisher(),
                    ).process(events, ScanMode.REINDEX)
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
            publisher = createLogRecordEventPublisher()
        ).collect {
            logger.info("block $blockNumber was re-indexed")
        }
    }

    private fun createLogService(): LogService<EthereumLogRecord, EthereumDescriptor> {
        return object : LogService<EthereumLogRecord, EthereumDescriptor> {
            override suspend fun delete(descriptor: EthereumDescriptor, record: EthereumLogRecord): EthereumLogRecord {
                throw IllegalStateException("Should not be called")
            }

            override suspend fun save(
                descriptor: EthereumDescriptor,
                records: List<EthereumLogRecord>,
                blockHash: String,
            ): List<EthereumLogRecord> {
                return records
            }

            override suspend fun prepareLogsToRevertOnNewBlock(
                descriptor: EthereumDescriptor,
                fullBlock: FullBlock<*, *>
            ): List<EthereumLogRecord> {
                return logService.prepareLogsToRevertOnNewBlock(descriptor, fullBlock)
            }

            override suspend fun prepareLogsToRevertOnRevertedBlock(
                descriptor: EthereumDescriptor,
                revertedBlockHash: String
            ): List<EthereumLogRecord> {
                throw IllegalStateException("Should not be called")
            }
        }
    }

    private fun createLogRecordEventPublisher(): LogRecordEventPublisher {
        return object : LogRecordEventPublisher {
            override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {
                onReconciliationListeners.forEach { listener -> listener.onLogRecordEvent(groupId, logRecordEvents) }
            }
        }
    }

    private fun createEmptyLogRecordEventPublisher(): LogRecordEventPublisher {
        return object : LogRecordEventPublisher {
            override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {}
        }
    }
}
