package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.BlockBatcher
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
class BlockEventProcessor<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    logMapper: LogMapper<BB, BL, L>,
    logService: LogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(BlockEventProcessor::class.java)

    private val subscribers = ArrayList<BlockEventSubscriber<BB, BL, L, R, D>>()

    init {
        logger.info("Injecting {} subscribers", subscribers.size)

        for (subscriber in subscribers) {
            val blockEventSubscriber = BlockEventSubscriber(
                blockchainClient,
                subscriber,
                logMapper,
                logService
            )
            this.subscribers.add(blockEventSubscriber)

            logger.info("Injected {} subscriber into BlockEventHandler", blockEventSubscriber)
        }
    }

    suspend fun onBlockEvents(events: List<BlockEvent>): Flow<Pair<BlockEvent, MutableList<R>>> = coroutineScope {
        val batches = BlockBatcher.toBatches(events)
        logger.info("Split BlockEvents {} into {} batches", events.map { it.number }, batches.size)

        // Processing each batch and flatter them into consequent flow
        batches.flatMap { batch ->
            processBlockEventsBatch(batch)
        }.asFlow()
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun processBlockEventsBatch(batch: List<BlockEvent>): List<Pair<BlockEvent, MutableList<R>>> =
        coroutineScope {
            val blockNumbers = batch.map { it.number }
            logger.info("Processing {} subscribers with BlockEvent batch: {}", subscribers.size, blockNumbers)

            val bySubscriber = subscribers.map { subscriber ->
                async {
                    val label = subscriber.subscriber.javaClass.name
                    val descriptor = subscriber.descriptor
                    descriptor to withSpan("processSingleSubscriber", labels = listOf("subscriber" to label)) {
                        when (batch[0]) {
                            is NewBlockEvent -> subscriber.onNewBlockEvents(batch as List<NewBlockEvent>)
                            is RevertedBlockEvent -> subscriber.onRevertedBlockEvents(batch as List<RevertedBlockEvent>)
                            is ReindexBlockEvent -> subscriber.onReindexBlockEvents(batch as List<ReindexBlockEvent>)
                        }
                    }
                }
            }.awaitAll()

            logger.info("BlockEvent batch processed: {}", blockNumbers)

            // Reverting relation "Subscriber -> BlockEvent[]" to "BlockEvent -> Subscriber[]"
            val byBlock = HashMap<BlockEvent, MutableList<R>>()
            bySubscriber.forEach { subscriberLogEvent ->
                val logs = subscriberLogEvent.second
                logs.forEach {
                    byBlock.computeIfAbsent(it.key) { mutableListOf() }.addAll(it.value)
                }
            }

            batch.map { it to byBlock.getOrDefault(it, mutableListOf()) }
        }


}
