package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockEventProcessor<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>,
    pendingLogService: PendingLogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(BlockEventProcessor::class.java)

    private val subscribers = ArrayList<BlockEventSubscriber<BB, BL, L, R, D>>()

    private val pendingLogMarker = PendingLogMarker(
        logService,
        pendingLogService
    )

    init {
        logger.info("Injecting {} subscribers", subscribers.size)

        for (subscriber in subscribers) {
            val blockEventSubscriber = BlockEventSubscriber(
                blockchainClient,
                subscriber,
                logMapper,
                logService,
                pendingLogMarker
            )
            this.subscribers.add(blockEventSubscriber)

            logger.info("Injected {} subscriber into BlockEventHandler", blockEventSubscriber)
        }
    }

    suspend fun onBlockEvents(events: List<BlockEvent>): Flow<Map<BlockEvent, List<R>>> = coroutineScope {
        val batches = toBatches(events)
        logger.info("Split BlockEvents {} into {} batches", events.map { it.number }, batches.size)

        // Batches are ordered in same way as BlockEvents in received List, so we can emit results batch-by-batch
        batches.map { batch ->
            val batchLogs = processBlockEventsBatch(batch)
            // Now we need to combine ALL subscriber's logs, grouped by BlockEvent to emit them in right order
            batch.associateByTo(LinkedHashMap(), { it }, { batchLogs.getOrDefault(it, emptyList()) })
        }.asFlow()
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun processBlockEventsBatch(batch: List<BlockEvent>): Map<BlockEvent, List<R>> = coroutineScope {
        val blockNumbers = batch.map { it.number }
        logger.info("Processing {} subscribers with BlockEvent batch: {}", subscribers.size, blockNumbers)

        val bySubscriber = subscribers.map { subscriber ->
            async {
                val label = subscriber.subscriber.javaClass.name
                // TODO how we can change it?
                withSpan("processSingleSubscriber", labels = listOf("subscriber" to label)) {
                    when (batch[0]) {
                        is NewBlockEvent -> subscriber.onNewBlockEvents(batch as List<NewBlockEvent>)
                        is RevertedBlockEvent -> subscriber.onRevertedBlockEvents(batch as List<RevertedBlockEvent>)
                    }
                }
            }
        }.awaitAll()

        logger.info("BlockEvent batch processed: {}", blockNumbers)

        val result = HashMap<BlockEvent, MutableList<R>>()
        bySubscriber.forEach { subscriberLogs ->
            subscriberLogs.forEach {
                result.computeIfAbsent(it.key) { ArrayList() }.addAll(it.value)
            }
        }
        result
    }

    private fun toBatches(events: List<BlockEvent>): List<List<BlockEvent>> {
        val batches = mutableListOf<List<BlockEvent>>()
        val iterator = events.iterator()
        var current = iterator.next()
        var currentBatch = mutableListOf(current)
        while (iterator.hasNext()) {
            val next = iterator.next()
            if (next.javaClass == current.javaClass) {
                currentBatch.add(next)
            } else {
                batches.add(currentBatch)
                currentBatch = mutableListOf(next)
            }
            current = next
        }
        batches.add(currentBatch)
        return batches
    }

}
