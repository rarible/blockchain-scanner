package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.LogEventProcessor
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.LogEvent
import org.springframework.beans.factory.annotation.Value
import reactor.core.publisher.Flux

class ReconciliationBlockIndexerService<OB : BlockchainBlock, OL, L : LogEvent, D : EventData>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    logEventProcessors: List<LogEventProcessor<OB, OL, L, D>>,
    @Value("\${ethereumBlockBatchSize:100}") private val batchSize: Long
) {

    private val blockIndexers = logEventProcessors.associate {
        it.subscriber.getDescriptor().topic to ReconciliationBlockIndexer(blockchainClient, it, batchSize)
    }

    fun reindex(topic: String?, from: Long, to: Long): Flux<LongRange> {
        val blockIndexer = blockIndexers[topic]
            ?: throw IllegalArgumentException(
                "BlockIndexer for topic '$topic' not found," +
                        " available topics: ${blockIndexers.keys}"
            )

        return blockIndexer.reindex(from, to)
    }
}