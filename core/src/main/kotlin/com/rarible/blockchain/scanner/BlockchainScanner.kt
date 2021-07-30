package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.pending.DefaultPendingBlockChecker
import com.rarible.blockchain.scanner.pending.DefaultPendingLogChecker
import com.rarible.blockchain.scanner.pending.PendingBlockChecker
import com.rarible.blockchain.scanner.pending.PendingLogChecker
import com.rarible.blockchain.scanner.reconciliation.ReconciliationExecutor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventPostProcessor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
open class BlockchainScanner<OB : BlockchainBlock, OL : BlockchainLog, B : Block, L : Log>(
    blockchainClient: BlockchainClient<OB, OL>,
    subscribers: List<LogEventSubscriber<OL, OB>>,
    blockMapper: BlockMapper<OB, B>,
    blockService: BlockService<B>,
    logMapper: LogMapper<OL, OB, L>,
    logService: LogService<L>,
    logEventListeners: List<LogEventListener<L>>,
    pendingLogService: PendingLogService<OB, L>,
    logEventPostProcessors: List<LogEventPostProcessor<L>>,
    properties: BlockchainScannerProperties

) : PendingLogChecker, BlockListener, PendingBlockChecker, ReconciliationExecutor {

    private val blockScanner = BlockScanner(
        blockchainClient,
        blockMapper,
        blockService,
        properties
    )

    private val blockListener = BlockEventListener(
        blockchainClient,
        subscribers,
        blockService,
        logMapper,
        logService,
        logEventListeners,
        pendingLogService,
        logEventPostProcessors,
        properties
    )

    private val pendingLogChecker = DefaultPendingLogChecker(
        blockchainClient,
        blockListener,
        subscribers.map { it.getDescriptor().collection }.toSet(),
        logService,
        logEventPostProcessors
    )

    private val pendingBlockChecker = DefaultPendingBlockChecker(
        blockchainClient,
        blockService,
        blockListener
    )

    private val reconciliationService = ReconciliationService(
        blockchainClient,
        subscribers,
        logMapper,
        logService,
        logEventListeners,
        properties
    )

    private val topics = subscribers.map { it.getDescriptor().topic }.toSet()

    // TODO should be called in onApplicationStartedEvent in implementations
    fun scan() = runBlocking {
        blockScanner.scan(blockListener)
    }

    override suspend fun onBlockEvent(event: BlockEvent) {
        return blockListener.onBlockEvent(event)
    }

    override fun checkPendingLogs() {
        pendingLogChecker.checkPendingLogs()
    }

    override fun checkPendingBlocks() {
        pendingBlockChecker.checkPendingBlocks()
    }

    override suspend fun reconcile(topic: String?, from: Long): Flow<LongRange> {
        return reconciliationService.reindex(topic, from)
    }

    override fun getTopics(): Set<String> {
        return topics
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BlockchainScanner::class.java)
    }
}