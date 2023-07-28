package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.core.logging.RaribleMDCContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BlockReindexer<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
    private val blockHandlerFactory: BlockHandlerFactory<BB, BL, R, D>,
    private val logHandlerFactory: LogHandlerFactory<BB, BL, R, D>
) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val reindexLogRecordEventPublisher = object : LogRecordEventPublisher {
        override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {
            val blockNumber = logRecordEvents.firstOrNull()?.record?.getBlock()
            logger.info("Re-indexed log events for block $blockNumber: groupId=$groupId, size=${logRecordEvents.size}")
        }
    }

    suspend fun reindex(
        baseBlock: Block,
        blocksRanges: Flow<TypedBlockRange>,
        filter: SubscriberFilter<BB, BL, R, D>? = null,
        publisher: LogRecordEventPublisher? = null
    ): Flow<Block> {
        return withContext(RaribleMDCContext(mapOf("reindex-task" to "true"))) {

            val wrappedSubscribers = filter?.filter(subscribers) ?: subscribers
            val selectedPublisher = publisher ?: reindexLogRecordEventPublisher

            val logHandlers = wrappedSubscribers
                .groupBy { it.getDescriptor().groupId }
                .map { (groupId, subscribers) ->
                    logger.info(
                        "Reindex with subscribers of the group {}: {}",
                        groupId,
                        subscribers.joinToString { it.getDescriptor().toString() }
                    )
                    logHandlerFactory.create(
                        groupId = groupId,
                        subscribers = subscribers,
                        logRecordEventPublisher = selectedPublisher,
                    )
                }

            val blockHandler = blockHandlerFactory.create(logHandlers)
            blockHandler.syncBlocks(blocksRanges, baseBlock, resyncStable = true)
        }
    }
}
