package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import org.slf4j.LoggerFactory

abstract class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    private val subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    private val logFilters: List<LogEventFilter<R, D>>,
    private val blockService: BlockService,
    private val logService: LogService<R, D>,
    private val logRecordComparator: LogRecordComparator<R>,
    private val properties: BlockchainScannerProperties,
    private val logRecordEventPublisher: LogRecordEventPublisher,
    private val blockMonitor: BlockMonitor,
    private val logMonitor: LogMonitor
) {

    private val retryableClient = RetryableBlockchainClient(
        original = blockchainClient,
        retryPolicy = properties.retryPolicy.client
    )


    suspend fun scan(once: Boolean = false) {
        if (!properties.scan.enabled) {
            logger.info("Blockchain scanning is disabled")
            return
        }
        val retryOnFlowCompleted: RetryPolicy<Throwable> = {
            logger.warn("Blockchain scanning interrupted with cause:", reason)
            logger.info("Will try to reconnect to blockchain in ${properties.retryPolicy.scan.reconnectDelay}")
            ContinueRetrying
        }
        val logHandlers = subscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                logRecordEventPublisher.prepareGroup(groupId)

                logger.info("Injected subscribers of the group {}: {}", groupId, subscribers.joinToString { it.getDescriptor().id })
                LogHandler(
                    groupId = groupId,
                    blockchainClient = retryableClient,
                    subscribers = subscribers,
                    logService = logService,
                    logRecordComparator = logRecordComparator,
                    logRecordEventPublisher = logRecordEventPublisher,
                    logFilters = logFilters,
                    logMonitor = logMonitor
                )
            }
        val blockHandler = BlockHandler(
            blockClient = retryableClient,
            blockService = blockService,
            blockEventListeners = logHandlers,
            scanProperties = properties.scan,
            monitor = blockMonitor
        )
        val maxAttempts = properties.retryPolicy.scan.reconnectAttempts.takeIf { it > 0 } ?: Integer.MAX_VALUE
        val delayMillis = properties.retryPolicy.scan.reconnectDelay.toMillis()
        if (once) {
            retryableClient.newBlocks.collect { blockHandler.onNewBlock(it) }
            return
        }
        retry(retryOnFlowCompleted + limitAttempts(maxAttempts) + constantDelay(delayMillis)) {
            logger.info("Connecting to blockchain...")
            retryableClient.newBlocks.collect { blockHandler.onNewBlock(it) }
            throw IllegalStateException("Disconnected from Blockchain, event flow completed")
        }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(BlockchainScanner::class.java)
    }
}
