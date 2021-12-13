package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.bouncycastle.util.encoders.Hex
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.ZoneOffset

/**
 * Client for Flow blockchain
 */
@FlowPreview
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
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

    override fun getBlockLogs(
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
                            event = flowEvent,
                            index = 0 // changed below
                        )
                    }.sortedBy { it.hash }
                        .sortedBy { it.event.eventIndex }
                        .withIndex().map { it.value.copy(index = it.index) }.toList()
                    // TODO: ^ please rethink/review this code that sets index.

                    send(
                        FullBlock(
                            block = entry.key,
                            logs = logs
                        )
                    )
                }
        }
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val tx = api.txById(transactionHash) ?: return null
        return TransactionMeta(hash = transactionHash, blockHash = Hex.toHexString(tx.referenceBlockId.bytes))
    }
}
