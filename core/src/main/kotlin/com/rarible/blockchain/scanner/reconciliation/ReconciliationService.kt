package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.LogEventPublisher
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

/**
 * Reconciliation service contains set of single Reconciliation Indexers(one per subscriber) and
 * triggers reconciliation procedure for any of them from specified block until last known.
 */
@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationService<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    logEventPublisher: LogEventPublisher<L, R>,
    properties: BlockchainScannerProperties
) {

    // Making single LogEventHandler for each subscriber
    private val indexers = subscribers.map {
        LogEventHandler(it, logMapper, logService)
    }.associate {
        it.subscriber.getDescriptor().id to createIndexer(it, logEventPublisher, properties)
    }

    fun reindex(descriptorId: String?, from: Long): Flow<LongRange> = flow {
        val lastBlockNumber = blockchainClient.getLastBlockNumber()
        emitAll(reindex(descriptorId, from, lastBlockNumber))
    }

    private fun reindex(descriptorId: String?, from: Long, to: Long): Flow<LongRange> {
        val blockIndexer = indexers[descriptorId]
            ?: throw IllegalArgumentException(
                "BlockIndexer for descriptor '$descriptorId' not found," +
                        " available descriptors: ${indexers.keys}"
            )

        return blockIndexer.reindex(from, to)
    }

    private fun createIndexer(
        logEventHandler: LogEventHandler<BB, BL, L, R, D>,
        logEventPublisher: LogEventPublisher<L, R>,
        properties: BlockchainScannerProperties
    ): ReconciliationIndexer<BB, BL, L, R, D> {
        return ReconciliationIndexer(
            blockchainClient,
            logEventHandler,
            logEventPublisher,
            properties.batchSize
        )
    }
}