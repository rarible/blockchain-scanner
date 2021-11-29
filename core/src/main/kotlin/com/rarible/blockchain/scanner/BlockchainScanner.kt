package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.consumer.DefaultBlockEventConsumer
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
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.pending.DefaultPendingBlockChecker
import com.rarible.blockchain.scanner.pending.DefaultPendingLogChecker
import com.rarible.blockchain.scanner.pending.PendingBlockChecker
import com.rarible.blockchain.scanner.pending.PendingLogChecker
import com.rarible.blockchain.scanner.publisher.DefaultBlockEventPublisher
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import java.time.Duration

@FlowPreview
@ExperimentalCoroutinesApi
open class BlockchainScanner<BB : BlockchainBlock, BL : BlockchainLog, B : Block, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    blockMapper: BlockMapper<BB, B>,
    blockService: BlockService<B>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    pendingLogService: PendingLogService<BB, L, R, D>,
    logEventListeners: List<LogEventListener<L, R>>,
    properties: BlockchainScannerProperties
) : PendingLogChecker, PendingBlockChecker, ReconciliationExecutor {

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

    // TODO replace with Kafka
    private val blockEventConsumer = DefaultBlockEventConsumer()
    private val blockEventPublisher = DefaultBlockEventPublisher(blockEventConsumer)

    private val blockEventListener = BlockEventListener(
        retryableBlockchainClient,
        subscribers,
        blockService,
        logMapper,
        logService,
        pendingLogService,
        logEventPublisher
    )

    private val pendingLogChecker = DefaultPendingLogChecker(
        retryableBlockchainClient,
        logService,
        subscribers.map { it.getDescriptor() },
        blockEventListener,
        logEventListeners
    )

    private val pendingBlockChecker = DefaultPendingBlockChecker(
        retryableBlockchainClient,
        blockService,
        blockEventListener
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
        blockEventConsumer.start(blockEventListener)
        blockScanner.scan(blockEventPublisher)
    }

    override suspend fun checkPendingLogs() {
        pendingLogChecker.checkPendingLogs()
    }

    override suspend fun checkPendingBlocks(pendingBlockAgeToCheck: Duration) {
        pendingBlockChecker.checkPendingBlocks(pendingBlockAgeToCheck)
    }

    override fun reconcile(descriptorId: String?, from: Long, batchSize: Long): Flow<LongRange> {
        return reconciliationService.reindex(descriptorId, from, batchSize)
    }

    override fun getDescriptorIds(): Set<String> {
        return descriptorIds
    }
}
