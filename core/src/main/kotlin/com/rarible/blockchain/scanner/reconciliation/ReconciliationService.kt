package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow

@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationService<BB : BlockchainBlock, BL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<BB, BL>,
    subscribers: List<LogEventSubscriber<BB, BL>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L>,
    logEventListeners: List<LogEventListener<L>>,
    properties: BlockchainScannerProperties
) {

    private val indexers = subscribers.map {
        LogEventHandler(it, logMapper, logService, logEventListeners)
    }.associate {
        it.subscriber.getDescriptor().topic to createIndexer(it, properties)
    }

    suspend fun reindex(topic: String?, from: Long): Flow<LongRange> {
        val blockNumber = blockchainClient.getLastBlockNumber()
        return reindex(topic, from, blockNumber)
    }

    private fun reindex(topic: String?, from: Long, to: Long): Flow<LongRange> {
        val blockIndexer = indexers[topic]
            ?: throw IllegalArgumentException(
                "BlockIndexer for topic '$topic' not found," +
                        " available topics: ${indexers.keys}"
            )

        return blockIndexer.reindex(from, to)
    }

    private fun createIndexer(
        logEventHandler: LogEventHandler<BB, BL, L>,
        properties: BlockchainScannerProperties
    ): ReconciliationIndexer<BB, BL, L> {
        return ReconciliationIndexer(
            blockchainClient,
            logEventHandler,
            properties.batchSize
        )
    }
}