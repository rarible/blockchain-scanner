package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.event.block.BlockScanner
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow

@FlowPreview
@ExperimentalCoroutinesApi
open class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, B : Block, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    blockMapper: BlockMapper<BB, B>,
    blockService: BlockService<B>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    properties: BlockchainScannerProperties,
    // Autowired beans
    private val blockEventPublisher: BlockEventPublisher,
    private val blockEventConsumer: BlockEventConsumer,
    logEventPublisher: LogEventPublisher
) : ReconciliationExecutor {

    private val retryableBlockchainClient = RetryableBlockchainClient(
        blockchainClient,
        properties.retryPolicy.client
    )

    private val blockScanner = BlockScanner(
        blockMapper,
        retryableBlockchainClient,
        blockService,
        properties.retryPolicy.scan
    )

    protected val blockEventListeners = subscribers
        .groupBy { it.getDescriptor().groupId }
        .map {
            it.key to BlockEventListener(
                retryableBlockchainClient,
                it.value,
                logMapper,
                logService,
                logEventPublisher
            )
        }.associateBy({ it.first }, { it.second })

    private val reconciliationService = ReconciliationService(
        blockService = blockService,
        blockEventListeners = blockEventListeners
    )

    private val descriptorIds = subscribers.map { it.getDescriptor().id }.toSet()

    suspend fun scan() {
        blockEventConsumer.start(blockEventListeners)
        blockScanner.scan(blockEventPublisher)
    }

    override fun reconcile(subscriberGroupId: String?, from: Long, batchSize: Long): Flow<LongRange> {
        return reconciliationService.reindex(subscriberGroupId, from, batchSize)
    }

    override fun getSubscriberGroupIds(): Set<String> {
        return descriptorIds
    }
}
