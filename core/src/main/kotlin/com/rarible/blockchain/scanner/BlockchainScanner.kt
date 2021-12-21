package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.event.block.BlockScanner
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import kotlinx.coroutines.flow.Flow

open class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, B : Block, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    blockMapper: BlockMapper<BB, B>,
    blockService: BlockService<B>,
    logService: LogService<L, R, D>,
    logEventComparator: LogEventComparator<L, R>,
    properties: BlockchainScannerProperties,
    // Autowired beans
    private val blockEventPublisher: BlockEventPublisher,
    private val blockEventConsumer: BlockEventConsumer,
    logEventPublisher: LogEventPublisher
) : ReconciliationExecutor {

    private val retryableClient = RetryableBlockchainClient(blockchainClient, properties.retryPolicy.client)

    private val blockScanner = BlockScanner(
        blockMapper,
        retryableClient,
        blockService,
        properties.retryPolicy.scan
    )

    private val blockEventListeners = subscribers
        .groupBy { it.getDescriptor().groupId }
        .map {
            it.key to BlockEventListener(
                blockchainClient = retryableClient,
                subscribers = it.value,
                logService = logService,
                logEventComparator = logEventComparator,
                logEventPublisher = logEventPublisher
            )
        }.associateBy({ it.first }, { it.second })

    private val reconciliationService = ReconciliationService(
        blockService = blockService,
        blockEventListeners = blockEventListeners
    )

    private val descriptors = subscribers.map { it.getDescriptor() }

    suspend fun scan() {
        blockEventConsumer.start(blockEventListeners)
        blockScanner.scan(blockEventPublisher)
    }

    override fun reconcile(groupId: String, from: Long, batchSize: Long): Flow<LongRange> =
        reconciliationService.reindex(groupId, from, batchSize)

    override fun getDescriptors(): List<Descriptor> = descriptors
}
