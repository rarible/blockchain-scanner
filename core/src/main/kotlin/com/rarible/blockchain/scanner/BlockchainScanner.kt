package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.event.block.BlockScanner
import com.rarible.blockchain.scanner.event.block.BlockService
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import kotlinx.coroutines.flow.Flow

abstract class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    blockService: BlockService,
    logService: LogService<R, D>,
    logRecordComparator: LogRecordComparator<R>,
    private val properties: BlockchainScannerProperties,
    private val blockEventPublisher: BlockEventPublisher,
    private val blockEventConsumer: BlockEventConsumer,
    logRecordEventPublisher: LogRecordEventPublisher
) : ReconciliationExecutor {

    private val retryableClient = RetryableBlockchainClient(blockchainClient, properties.retryPolicy.client)

    private val blockScanner = BlockScanner(
        retryableClient,
        blockService,
        properties.retryPolicy.scan,
        properties.scan.blockConsume.batchLoad
    )

    private val blockEventListeners = subscribers
        .groupBy { it.getDescriptor().groupId }
        .map {
            it.key to BlockEventListener(
                blockchainClient = retryableClient,
                subscribers = it.value,
                logService = logService,
                logRecordComparator = logRecordComparator,
                logRecordEventPublisher = logRecordEventPublisher
            )
        }.associateBy({ it.first }, { it.second })

    private val reconciliationService = ReconciliationService(
        blockService = blockService,
        blockEventListeners = blockEventListeners
    )

    private val descriptors = subscribers.map { it.getDescriptor() }

    suspend fun scan() {
        if (properties.scan.logConsume.enabled) {
            blockEventConsumer.start(blockEventListeners)
        }
        if (properties.scan.blockPublish.enabled) {
            blockScanner.scan(blockEventPublisher)
        }
    }

    override fun reconcile(groupId: String, from: Long, batchSize: Long): Flow<LongRange> =
        reconciliationService.reindex(groupId, from, batchSize)

    override fun getDescriptors(): List<Descriptor> = descriptors
}
