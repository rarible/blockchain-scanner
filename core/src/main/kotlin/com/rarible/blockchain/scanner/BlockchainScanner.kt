package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.handler.TransactionHandler
import org.slf4j.LoggerFactory

abstract class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, TR : TransactionRecord, D : Descriptor>(
    manager: BlockchainScannerManager<BB, BL, R, TR, D>
) {

    private val retryableClient = manager.retryableClient
    private val logSubscribers = manager.logSubscribers
    private val blockService = manager.blockService
    private val blockMonitor = manager.blockMonitor
    private val logFilters = manager.logFilters
    private val logService = manager.logService
    private val logRecordComparator = manager.logRecordComparator
    private val logRecordEventPublisher = manager.logRecordEventPublisher
    private val logMonitor = manager.logMonitor
    private val properties = manager.properties
    private val transactionRecordEventPublisher = manager.transactionRecordEventPublisher
    private val transactionSubscribers = manager.transactionSubscribers

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
        val logHandlers = logSubscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                logRecordEventPublisher.prepareGroup(groupId)

                logger.info(
                    "Injected log subscribers of the group {}: {}",
                    groupId,
                    subscribers.joinToString { it.getDescriptor().id })
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
        val transactionHandlers = transactionSubscribers
            .groupBy { it.getGroup() }
            .map { (groupId, subscribers) ->
                transactionRecordEventPublisher.prepareGroup(groupId)

                logger.info(
                    "Injected transaction subscribers of the group {}: {}",
                    groupId,
                    subscribers.joinToString { it::class.java.simpleName })
                TransactionHandler(
                    groupId = groupId,
                    subscribers = subscribers,
                    transactionRecordEventPublisher = transactionRecordEventPublisher,
                )
            }
        val blockHandler = BlockHandler(
            blockClient = retryableClient,
            blockService = blockService,
            blockEventListeners = logHandlers + transactionHandlers,
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
