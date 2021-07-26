package com.rarible.blockchain.scanner.service.reindex

import com.rarible.blockchain.scanner.client.BlockchainClient
import com.rarible.blockchain.scanner.model.EventData
import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.processor.LogEventProcessor
import org.springframework.beans.factory.annotation.Value
import reactor.core.publisher.Flux

class BlockIndexerService<OB, OL, L : LogEvent, D : EventData>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    logEventProcessors: List<LogEventProcessor<OB, OL, L, D>>,
    @Value("\${ethereumBlockBatchSize:100}") private val batchSize: Long
) {

    private val blockIndexers = logEventProcessors.associate {
        it.subscriber.topic to BlockIndexer(blockchainClient, it, batchSize)
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