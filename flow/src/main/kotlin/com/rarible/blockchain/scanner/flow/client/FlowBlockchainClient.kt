package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowEventResult
import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.http.FlowHttpApi
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
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
@ExperimentalCoroutinesApi
class FlowBlockchainClient(
    private val poller: FlowNetNewBlockPoller,
    private val api: FlowGrpcApi,
    private val httpApi: FlowHttpApi,
    private val properties: FlowBlockchainScannerProperties
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(FlowBlockchainClient::class.java)

    private val firstAvailableBlock = properties.scan.firstAvailableBlock

    override val newBlocks = flatten {
        poller.poll().map { FlowBlockchainBlock(it) }
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val a = api.blockByHeight(number) ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a)
    }

    override suspend fun getBlocks(numbers: List<Long>) = coroutineScope {
        numbers.map { asyncWithTraceId(context = NonCancellable) { getBlock(it) } }.awaitAll()
    }

    override fun getBlockLogs(
        descriptor: FlowDescriptor,
        blocks: List<FlowBlockchainBlock>,
        stable: Boolean,
        mode: ScanMode,
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> {
        // Normally, we have only one consequent range here.
        val ranges = BlockRanges.toRanges(blocks.map { it.number }).asFlow()
        return ranges.map { getBlockLogs(descriptor, it, mode) }.flattenConcat()
    }

    private fun getBlockLogs(
        descriptor: FlowDescriptor,
        range: LongRange,
        mode: ScanMode,
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> = channelFlow {
        logger.info("Get events from block range ${range.first}..${range.last} for ${descriptor.id}")
        api.chunk(range).collect { sl ->
            descriptor.events.map {
                asyncWithTraceId(context = NonCancellable) { eventsByBlockRange(it, sl, mode) }
            }.awaitAll()
                .flatMap { it.toList() }
                .filter { it.events.isNotEmpty() }
                .groupBy {
                    FlowBlockchainBlock(
                        number = it.blockHeight,
                        hash = it.blockId.base16Value,
                        timestamp = it.blockTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli(),
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

    private suspend fun eventsByBlockRange(type: String, range: LongRange, mode: ScanMode): Flow<FlowEventResult> {
        return if (properties.enableHttpClient || mode == ScanMode.REINDEX || mode == ScanMode.REINDEX_PARTIAL) httpApi.eventsByBlockRange(type, range)
        else api.eventsByBlockRange(type, range)
    }

    override suspend fun getFirstAvailableBlock(): FlowBlockchainBlock = getBlock(firstAvailableBlock)

    override suspend fun getLastBlockNumber(): Long {
        return api.latestBlock().height
    }
}
