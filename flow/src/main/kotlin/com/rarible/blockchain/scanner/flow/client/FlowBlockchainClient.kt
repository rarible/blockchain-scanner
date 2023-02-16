package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.ZoneOffset

/**
 * Client for Flow blockchain
 */
@Component
class FlowBlockchainClient(
    private val poller: FlowNetNewBlockPoller,
    private val api: FlowGrpcApi
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(FlowBlockchainClient::class.java)

    override val newBlocks = flatten {
        poller.poll(api.latestBlock().height).map { FlowBlockchainBlock(it) }
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val a = api.blockByHeight(number) ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a)
    }

    override suspend fun getBlocks(numbers: List<Long>) = coroutineScope {
        numbers.map { async { getBlock(it) } }.awaitAll()
    }

    override fun getBlockLogs(
        descriptor: FlowDescriptor,
        blocks: List<FlowBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> {
        // Normally, we have only one consequent range here.
        val ranges = BlockRanges.toRanges(blocks.map { it.number }).asFlow()
        return ranges.map { getBlockLogs(descriptor, it) }.flattenConcat()
    }

    private fun getBlockLogs(
        descriptor: FlowDescriptor,
        range: LongRange
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> = channelFlow {
        logger.info("Get events from block range ${range.first}..${range.last}")
        api.chunk(range).collect { sl ->
            descriptor.events.map {
                async { api.eventsByBlockRange(it, sl) }
            }.awaitAll()
                .flatMap { it.toList() }
                .filter { it.events.isNotEmpty() }
                .groupBy {
                    FlowBlockchainBlock(
                        number = it.blockHeight,
                        hash = it.blockId.base16Value,
                        timestamp = it.blockTimestamp.toInstant(ZoneOffset.UTC).epochSecond,
                        parentHash = null
                    )
                }.forEach { entry ->
                    val logs = entry.value.flatMap {
                        it.events
                    }.asSequence().map { flowEvent ->
                        FlowBlockchainLog(
                            hash = flowEvent.transactionId.base16Value,
                            // TODO FLOW - add transaction index
                            blockHash = entry.key.hash,
                            event = flowEvent
                        )
                    }.sortedBy { it.hash }
                        .sortedBy { it.event.eventIndex }
                        .toList()

                    logger.info(
                        "Loaded ${logs.size} logs for events descriptor ${descriptor.id} for block ${entry.key.number}"
                    )
                    send(
                        FullBlock(
                            block = entry.key,
                            logs = logs
                        )
                    )
                }
        }
    }

    override suspend fun getFirstAvailableBlock(): FlowBlockchainBlock = getBlock(0)
}
