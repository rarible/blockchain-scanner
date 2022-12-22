package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.handler.HandlerPlanner
import com.rarible.blockchain.scanner.ethereum.handler.ReindexHandler
import com.rarible.blockchain.scanner.ethereum.model.BlockRange
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.service.LogHandlerFactory
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.handler.BlocksRange
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.lang.IllegalStateException

@Component
@ExperimentalCoroutinesApi
class ReconciliationLogHandler(
    private val ethereumClient: EthereumBlockchainClient,
    private val logRepository: EthereumLogRepository,
    private val logHandlerFactory: LogHandlerFactory,
    private val logService: EthereumLogService,
    private val reindexHandler: ReindexHandler,
    private val handlerPlanner: HandlerPlanner,
    private val onReconciliationListeners: List<OnReconciliationListener>,
    private val scannerProperties: EthereumScannerProperties,
    subscribers: List<EthereumLogEventSubscriber>,
) {
    private val subscribersByCollection = subscribers.groupBy { subscriber -> subscriber.getDescriptor().collection }

    suspend fun check(blocksRange: BlocksRange) = coroutineScope {
        blocksRange.range.map { blockNumber ->
            async {
                handleBlock(blockNumber)
                blockNumber
            }
        }.awaitAll().last()
    }

    private suspend fun handleBlock(blockNumber: Long) = coroutineScope {
        subscribersByCollection.map { entity ->
            async { checkCollectionLogs(blockNumber, entity.key, entity.value) }
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
            logger.error("Saved logs count for block {} and collection '{}' are not consistent (saved={}, fetched={})",
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
        val events = listOf(NewStableBlockEvent(block))

        subscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                async {
                    logHandlerFactory.create(
                        groupId = groupId,
                        subscribers = subscribers,
                        logService = createLogService(),
                        logRecordEventPublisher = createLogRecordEventPublisher(),
                    ).process(events)
                }
            }
            .awaitAll()
            .fold(0L) { acc, result ->
                val inserted = result[blockNumber]?.inserted?.toLong() ?: error("Can't get stable block $blockNumber statistic")
                acc + inserted
            }
    }

    private suspend fun reindex(blockNumber: Long) {
        val blockRange = BlockRange(from = blockNumber, to = null, batchSize = null)
        val (reindexRanges, baseBlock) = handlerPlanner.getPlan(blockRange)
        reindexHandler.reindex(
            baseBlock = baseBlock,
            blocksRanges = reindexRanges,
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
                records: List<EthereumLogRecord>
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

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReconciliationLogHandler::class.java)
    }
}
