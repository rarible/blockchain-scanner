package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventHandler
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import reactor.core.publisher.Flux

class ReconciliationService<OB : BlockchainBlock, OL : BlockchainLog, L : Log, D : EventData>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    subscribers: List<LogEventSubscriber<OL, OB, D>>,
    logMapper: LogMapper<OL, OB, L>,
    logService: LogService<L>,
    logEventListeners: List<LogEventListener<L>>,
    private val properties: BlockchainScannerProperties
) {

    private val indexers = subscribers.map {
        LogEventHandler(it, logMapper, logService, logEventListeners)
    }.associate {
        it.subscriber.getDescriptor().topic to ReconciliationIndexer(blockchainClient, it, properties.batchSize)
    }

    fun reindex(topic: String?, from: Long): Flux<LongRange> {
        return blockchainClient.getLastBlockNumber()
            .flatMapMany { reindex(topic, from, it) }
    }

    private fun reindex(topic: String?, from: Long, to: Long): Flux<LongRange> {
        val blockIndexer = indexers[topic]
            ?: throw IllegalArgumentException(
                "BlockIndexer for topic '$topic' not found," +
                        " available topics: ${indexers.keys}"
            )

        return blockIndexer.reindex(from, to)
    }
}