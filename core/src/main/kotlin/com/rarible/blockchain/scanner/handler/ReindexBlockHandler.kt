package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.apm.SpanType
import com.rarible.core.apm.withSpan
import com.rarible.core.apm.withTransaction
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory

class ReindexBlockHandler<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val logService: LogService<R, D>,
    private val logRecordComparator: LogRecordComparator<R>,
    properties: BlockchainScannerProperties
) {
    private val batchSize = properties.scan.batchLoad.batchSize

    private val retryableClient = RetryableBlockchainClient(
        original = blockchainClient,
        retryPolicy = properties.retryPolicy.client
    )

    fun reindexBlocksLogs(
        blockRange: LongRange,
        subscribers: List<LogEventSubscriber<BB, BL, R, D>>,
        logRecordEventPublisher: LogRecordEventPublisher,
    ): Flow<BB> = flow  {
        val logHandlers = subscribers
            .groupBy { it.getDescriptor().groupId }
            .map { (groupId, subscribers) ->
                logger.info("Reindex with subscribers of the group {}: {}", groupId, subscribers.joinToString { it.getDescriptor().id })
                LogHandler(
                    blockchainClient = retryableClient,
                    subscribers = subscribers,
                    logService = logService,
                    logRecordComparator = logRecordComparator,
                    logRecordEventPublisher = logRecordEventPublisher
                )
            }

        logger.info("Reindex blocks starting from ${blockRange.first} to ${blockRange.last} with batches of size $batchSize")

        coroutineScope {
            BlockRanges.getRanges(
                from = blockRange.first,
                to = blockRange.last,
                step = batchSize
            ).map { range ->
                logger.info("Fetching blockchain blocks $range (${range.last - range.first + 1}) for reindex")
                withTransaction(
                    name = "fetchReindexBlocksBatch",
                    labels = listOf("range" to range.toString())
                ) {
                    range.map { id -> async { fetchBlock(id) } }.awaitAll()
                }
            }
                .map { it.filterNotNull() }
                .filter { it.isNotEmpty() }
                .map {
                    val fromId = it.first().number
                    val toId = it.last().number
                    logger.info("Processing batch of ${it.size} stable blocks with IDs between $fromId and $toId")
                    withTransaction(
                        name = "processReindexBlocks",
                        labels = listOf("batchSize" to it.size, "minId" to fromId, "maxId" to toId)
                    ) {
                        processBlocks(it, logHandlers)
                        emit(it.last())
                    }
                }
                .lastOrNull()
        }
    }

    private suspend fun fetchBlock(number: Long): BB? =
        withSpan(
            name = "fetchReindexBlock",
            type = SpanType.EXT,
            labels = listOf("blockNumber" to number)
        ) {
            blockchainClient.getBlock(number)
        }

    private suspend fun processBlocks(
        blockchainBlocks: List<BB>,
        logHandlers: List<LogHandler<BB, BL, R, D>>
    ) = coroutineScope<Unit> {
        if (blockchainBlocks.size == 1) {
            logger.info("Processing block [{}:{}]", blockchainBlocks.single().number, blockchainBlocks.single().hash)
        } else {
            logger.info("Processing blocks from {} to {}", blockchainBlocks.first().number, blockchainBlocks.last().number)
        }
        val events = blockchainBlocks.map {
            NewStableBlockEvent(it)
        }
        withSpan(
            name = "reindexLogs",
            labels = listOf(
                "blocksCount" to blockchainBlocks.size,
                "minId" to blockchainBlocks.first().number,
                "maxId" to blockchainBlocks.last().number
            )
        ) {
           logHandlers
               .map { async { it.process(events) } }
               .awaitAll()
               .lastOrNull()
        }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(ReindexBlockHandler::class.java)
    }
}
