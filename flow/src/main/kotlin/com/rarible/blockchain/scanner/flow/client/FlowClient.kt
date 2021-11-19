package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.model.BlockMeta
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
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
class FlowClient(
    private val poller: FlowNetNewBlockPoller,
    private val api: FlowGrpcApi
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(FlowClient::class.java)

    override val newBlocks = flatten {
        poller.poll(api.latestBlock().height).map { FlowBlockchainBlock(it.blockMeta()) }
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val a = api.blockByHeight(number) ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a.blockMeta())
    }


    override suspend fun getBlock(hash: String): FlowBlockchainBlock {
        val b = api.blockById(hash) ?: throw IllegalStateException("Block [$hash] not found!")
        return FlowBlockchainBlock(b.blockMeta())
    }

    override suspend fun getLastBlockNumber(): Long =
        api.latestBlock().height


    override fun getBlockEvents(
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
                    BlockMeta(
                        number = it.blockHeight,
                        hash = it.blockId.base16Value,
                        timestamp = it.blockTimestamp.toInstant(ZoneOffset.UTC).toEpochMilli(),
                        parentHash = null)
                }.forEach { entry ->
                    val logs = entry.value.flatMap {
                        it.events
                    }.map { flowEvent ->
                        FlowBlockchainLog(
                            hash = flowEvent.transactionId.base16Value,
                            blockHash = entry.key.hash,
                            event = flowEvent
                        )
                    }.sortedBy { it.hash }.sortedBy { it.event.eventIndex }

                    send(
                        FullBlock(
                            block = FlowBlockchainBlock(meta = entry.key),
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

    override fun getBlockEvents(
        descriptor: FlowDescriptor,
        block: FlowBlockchainBlock
    ): Flow<FlowBlockchainLog> {
        return api.blockEvents(height = block.number)
            .filter {
                descriptor.events.contains(it.type)
            }
            .map {
                FlowBlockchainLog(
                    hash = it.transactionId.base16Value,
                    blockHash = block.hash,
                    event = it
                )
            }
    }


}
