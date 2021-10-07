package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.service.LastReadBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.flow.*
import org.bouncycastle.util.encoders.Hex
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * Client for Flow blockchain
 */
@FlowPreview
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@Component
class FlowClient(
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId = DEFAULT_CHAIN_ID,
    private val poller: FlowNetNewBlockPoller,
    private val lastReadBlock: LastReadBlock,
    private val api: FlowGrpcApi
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(FlowClient::class.java)

    override fun listenNewBlocks(): Flow<FlowBlockchainBlock> = flow {
        emitAll(poller.poll(lastReadBlock.getLastReadBlockHeight()).map { FlowBlockchainBlock(it) })
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val a = api.blockByHeight(number) ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a)
    }


    override suspend fun getBlock(hash: String): FlowBlockchainBlock {
        val b = api.blockById(hash) ?: throw IllegalStateException("Block [$hash] not found!")
        return FlowBlockchainBlock(b)
    }

    override suspend fun getLastBlockNumber(): Long =
        api.latestBlock().height


    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        range: LongRange
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> = channelFlow {
        logger.info("Get events from block range ${range.first}..${range.last}")
        val chunkSize = when (chainId) {
            FlowChainId.MAINNET -> 250
            FlowChainId.TESTNET -> 25
            else -> 10
        }
        logger.info("Block's dia is $chunkSize")
        range.chunked(chunkSize) {
            LongRange(it.first(), it.last())
        }.forEach { smallRange ->
                val events =
                    api.eventsByBlockRange(descriptor.event, smallRange)
                events.map { eventResult ->
                    val logs = eventResult.events.map { flowEvent ->
                        FlowBlockchainLog(
                            hash = flowEvent.transactionId.base16Value,
                            blockHash = eventResult.blockId.base16Value,
                            event = flowEvent
                        )
                    }
                    if (logs.isNotEmpty()) {
                        logger.info("Received ${logs.size} events for type: ${descriptor.event} on range ${smallRange.first}..${smallRange.last}")
                        val block = api.blockById(eventResult.blockId)!!
                        send(
                            FullBlock(
                                block = FlowBlockchainBlock(block),
                                logs = logs
                            )
                        )
                    }
                }
        }
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val tx = api.txById(transactionHash) ?: return null
        return TransactionMeta(hash = transactionHash, blockHash = Hex.toHexString(tx.referenceBlockId.bytes))
    }

    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        block: FlowBlockchainBlock
    ): Flow<FlowBlockchainLog> {
        return api.blockEvents(type = descriptor.event, block = block.block).flatMapConcat {
            it.events.asFlow().map {
                FlowBlockchainLog(
                    hash = Hex.toHexString(it.transactionId.bytes),
                    blockHash = block.hash,
                    event = it
                )
            }
        }
    }
}
