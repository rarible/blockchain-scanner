package com.rarible.blockchain.scanner.solana.handler

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.solana.client.SolanaClient
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.service.SolanaLogService
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogRecordComparator
import com.rarible.core.logging.RaribleMDCContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ReindexHandler(
    private val allSubscribers: List<SolanaLogEventSubscriber>,
    private val blockService: BlockService,
    solanaClient: SolanaClient,
    private val logService: SolanaLogService,
    private val blockchainScannerProperties: BlockchainScannerProperties,
    private val blockMonitor: BlockMonitor,
    private val logEventSubscriberExceptionResolver: SolanaLogEventSubscriberExceptionResolver,
    private val logMonitor: LogMonitor,
) {
    private val retryableClient = RetryableBlockchainClient(
        original = solanaClient,
        retryPolicy = blockchainScannerProperties.retryPolicy.client
    )

    suspend fun reindex(
        blocks: List<Long>
    ): Flow<Block> {
        return withContext(RaribleMDCContext(mapOf("solana-reindex-task" to "true"))) {
            val logHandlers = allSubscribers
                .groupBy { it.getDescriptor().groupId }
                .map { (groupId, subscribers) ->
                    logger.info(
                        "Reindex with subscribers of the group {}: {}",
                        groupId,
                        subscribers.joinToString { it.getDescriptor().id }
                    )

                    LogHandler(
                        groupId = groupId,
                        blockchainClient = retryableClient,
                        subscribers = subscribers,
                        logService = logService,
                        logRecordComparator = SolanaLogRecordComparator,
                        logRecordEventPublisher = reindexLogRecordEventPublisher,
                        logMonitor = logMonitor,
                        logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver
                    )
                }

            val blockHandler = BlockHandler(
                blockClient = retryableClient,
                blockService = blockService,
                blockEventListeners = logHandlers,
                scanProperties = blockchainScannerProperties.scan,
                monitor = blockMonitor
            )

            blockHandler.syncBlocks(blocks)
        }
    }

    private val reindexLogRecordEventPublisher = object : LogRecordEventPublisher {
        override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {
            val blockNumber = (logRecordEvents.firstOrNull()?.record as? SolanaLogRecord)?.log?.blockNumber

            logger.info("Re-indexed log events for block $blockNumber: groupId=$groupId, size=${logRecordEvents.size}")
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(ReindexHandler::class.java)
    }
}
