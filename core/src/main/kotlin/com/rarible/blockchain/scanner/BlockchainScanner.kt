package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.handler.LogHandler
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.handler.BlockHandler
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import kotlinx.coroutines.flow.collect
import org.slf4j.LoggerFactory

abstract class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    private val blockService: BlockService,
    private val logService: LogService<R, D>,
    logRecordComparator: LogRecordComparator<R>,
    private val properties: BlockchainScannerProperties,
    logRecordEventPublisher: LogRecordEventPublisher
) {

    private val retryableClient = RetryableBlockchainClient(
        original = blockchainClient,
        retryPolicy = properties.retryPolicy.client
    )

    private val logHandlers = subscribers
        .groupBy { it.getDescriptor().groupId }
        .map { (groupId, subscribers) ->
            logger.info("Injected subscribers of the group {}: {}", groupId, subscribers)
            LogHandler(
                blockchainClient = retryableClient,
                subscribers = subscribers,
                logService = logService,
                logRecordComparator = logRecordComparator,
                logRecordEventPublisher = logRecordEventPublisher
            )
        }

    suspend fun scan(once: Boolean = false) {
        val retryOnFlowCompleted: RetryPolicy<Throwable> = {
            logger.warn("Blockchain scanning interrupted with cause:", reason)
            logger.info("Will try to reconnect to blockchain in ${properties.retryPolicy.scan.reconnectDelay}")
            ContinueRetrying
        }
        val blockHandler = BlockHandler(
            blockClient = retryableClient,
            blockService = blockService,
            blockEventListeners = logHandlers,
            batchLoad = properties.scan.batchLoad
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
