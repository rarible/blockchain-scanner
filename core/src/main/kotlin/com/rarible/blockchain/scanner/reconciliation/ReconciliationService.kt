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
class ReconciliationService<OB : BlockchainBlock, OL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    subscribers: List<LogEventSubscriber<OL, OB>>,
    logMapper: LogMapper<OL, OB, L>,
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
        logEventHandler: LogEventHandler<OB, OL, L>,
        properties: BlockchainScannerProperties
    ): ReconciliationIndexer<OB, OL, L> {
        return ReconciliationIndexer(
            blockchainClient,
            logEventHandler,
            properties.batchSize
        )
    }
}