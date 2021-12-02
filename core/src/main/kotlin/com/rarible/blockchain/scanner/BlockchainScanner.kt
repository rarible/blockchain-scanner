package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.event.block.BlockScanner
import com.rarible.blockchain.scanner.event.log.BlockEventListener
import com.rarible.blockchain.scanner.event.log.LogEventPublisher
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
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
    logEventListeners: List<LogEventListener<L, R>>,
    properties: BlockchainScannerProperties,
    private val blockEventPublisher: BlockEventPublisher,
    private val blockEventConsumer: BlockEventConsumer
) : ReconciliationExecutor, BlockListener {

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

    private val logEventPublisher = LogEventPublisher(
        logEventListeners,
        properties.retryPolicy.scan
    )

    private val blockEventListeners = subscribers
        .groupBy { it.getDescriptor().groupId }
        .map {
            it.key to BlockEventListener(
                retryableBlockchainClient,
                it.value,
                blockService,
                logMapper,
                logService,
                logEventPublisher
            )
        }.associateBy({ it.first }, { it.second })

    private val blockEventListener = BlockEventListener(
        retryableBlockchainClient,
        subscribers,
        blockService,
        logMapper,
        logService,
        logEventPublisher
    )

    private val reconciliationService = ReconciliationService(
        blockchainClient = retryableBlockchainClient,
        subscribers = subscribers,
        logMapper = logMapper,
        logService = logService,
        logEventPublisher = logEventPublisher,
        blockService = blockService,
        blockMapper = blockMapper
    )

    private val descriptorIds = subscribers.map { it.getDescriptor().id }.toSet()

    suspend fun scan() {
        blockEventConsumer.start(blockEventListeners)
        blockScanner.scan(blockEventPublisher)
    }

    override suspend fun onBlockEvents(events: List<BlockEvent>) {
        blockEventListener.onBlockEvents(events)
    }

    override fun reconcile(descriptorId: String?, from: Long, batchSize: Long): Flow<LongRange> {
        return reconciliationService.reindex(descriptorId, from, batchSize)
    }

    override fun getDescriptorIds(): Set<String> {
        return descriptorIds
    }
}
