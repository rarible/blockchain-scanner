package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.ReindexBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.util.BlockBatcher
import com.rarible.core.apm.withSpan
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory

class BlockEventProcessor<BB : BlockchainBlock, BL : BlockchainLog, L : Log<L>, R : LogRecord<L, *>, D : Descriptor>(
    blockchainClient: BlockchainClient<BB, BL, D>,
    subscribers: List<LogEventSubscriber<BB, BL, L, R, D>>,
    private val logService: LogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(BlockEventProcessor::class.java)

    private val subscribers: List<BlockEventSubscriber<BB, BL, L, R, D>>

    init {
        this.subscribers = subscribers.map {
            BlockEventSubscriber(blockchainClient, it, logService)
        }
        logger.info("Injected subscribers: {}", this.subscribers)
    }

    suspend fun prepareBlockEvents(events: List<BlockEvent>): List<LogEvent<L, R, D>> {
        val batches = BlockBatcher.toBatches(events)
        return batches.flatMap { prepareBlockEventsBatch(it) }
    }

    suspend fun insertOrRemoveRecords(logEvents: List<LogEvent<L, R, D>>) = coroutineScope {
        logEvents.flatMap { logEvent ->
            listOf(
                async {
                    logService.delete(logEvent.descriptor, logEvent.logRecordsToRemove)
                },
                async {
                    logService.save(logEvent.descriptor, logEvent.logRecordsToInsert)
                }
            )
        }.awaitAll()
    }

    private suspend fun prepareBlockEventsBatch(batch: List<BlockEvent>): List<LogEvent<L, R, D>> =
        coroutineScope {
            logger.info("Processing {} subscribers with BlockEvent batch: {}", subscribers.size, batch)
            subscribers.map { subscriber ->
                async {
                    withSpan(
                        name = "processSubscriber",
                        labels = listOf("subscriber" to subscriber.subscriber.javaClass.name)
                    ) {
                        @Suppress("UNCHECKED_CAST")
                        when (batch[0]) {
                            is NewBlockEvent -> subscriber.onNewBlockEvents(batch as List<NewBlockEvent>)
                            is RevertedBlockEvent -> subscriber.onRevertedBlockEvents(batch as List<RevertedBlockEvent>)
                            is ReindexBlockEvent -> subscriber.onReindexBlockEvents(batch as List<ReindexBlockEvent>)
                        }
                    }
                }
            }.awaitAll().flatten()
        }


}
