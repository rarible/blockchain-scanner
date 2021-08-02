package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.BlockEventPostProcessor
import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow

@FlowPreview
@ExperimentalCoroutinesApi
class ReconciliationService<BB : BlockchainBlock, BL : BlockchainLog, L : Log>(
    private val blockchainClient: BlockchainClient<BB, BL>,
    subscribers: List<LogEventSubscriber<BB, BL>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L>,
    blockEventPostProcessor: BlockEventPostProcessor<L>,
    properties: BlockchainScannerProperties
) {

    private val indexers = subscribers.map {
        LogEventHandler(it, logMapper, logService)
    }.associate {
        it.subscriber.getDescriptor().topic to createIndexer(it, blockEventPostProcessor, properties)
    }

    fun reindex(topic: String?, from: Long): Flow<LongRange> {
        return flow {
            emit(blockchainClient.getLastBlockNumber())
        }.flatMapConcat {
            reindex(topic, from, it)
        }
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
        blockEventPostProcessor: BlockEventPostProcessor<L>,
        properties: BlockchainScannerProperties
    ): ReconciliationIndexer<BB, BL, L> {
        return ReconciliationIndexer(
            blockchainClient,
            logEventHandler,
            blockEventPostProcessor,
            properties.batchSize
        )
    }
}