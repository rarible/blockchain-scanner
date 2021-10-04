package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowId
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.service.LastReadBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import org.bouncycastle.util.encoders.Hex
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.Executors

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
    private val lastReadBlock: LastReadBlock
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private val dispatcher = Executors.newCachedThreadPool().asCoroutineDispatcher()

    override fun listenNewBlocks(): Flow<FlowBlockchainBlock> = flow {
        emitAll(poller.poll(lastReadBlock.getLastReadBlockHeight()).map { FlowBlockchainBlock(it) })
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val client = FlowAccessApiClientManager.async(number, chainId).asyncClient
        val a = client.getBlockByHeight(number).await() ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a)
    }


    override suspend fun getBlock(hash: String): FlowBlockchainBlock {
        val client = FlowAccessApiClientManager.async(hash, chainId).asyncClient
        val b = client.getBlockById(FlowId(hash)).await() ?: throw IllegalStateException("Block [$hash] not found!")
        return FlowBlockchainBlock(b)
    }

    override suspend fun getLastBlockNumber(): Long =
        FlowAccessApiClientManager.asyncForCurrentSpork(chainId).getLatestBlockHeader().await().height


    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        range: LongRange
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> {
        val sporks = FlowAccessApiClientManager.async(range, chainId).toList()
        return range.chunked(10).asFlow().map {
            val def = it.map { num ->
                val client = sporks.first { it.containsBlock(num) }.asyncClient
                client.getBlockByHeight(num).asDeferred()
            }
            def.awaitAll().filterNotNull()
        }.buffer().flatMapMerge {
            it.asFlow().map { fb ->
                val client = sporks.first { it.containsBlock(fb.height) }.asyncClient
                fullBlock(fb, client)
            }
        }

    }


    private suspend fun fullBlock(
        block: FlowBlock,
        client: AsyncFlowAccessApi
    ): FullBlock<FlowBlockchainBlock, FlowBlockchainLog> =
        FullBlock(
            block = FlowBlockchainBlock(block),
            logs = blockEvents(block, client).toList()
        )

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val client = FlowAccessApiClientManager.asyncByTxHash(transactionHash, chainId).asyncClient
        val tx = client.getTransactionById(FlowId(transactionHash)).await() ?: return null
        return TransactionMeta(hash = transactionHash, blockHash = Hex.toHexString(tx.referenceBlockId.bytes))
    }

    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        block: FlowBlockchainBlock
    ): Flow<FlowBlockchainLog> {
        val client = FlowAccessApiClientManager.async(block.number, chainId).asyncClient
        return blockEvents(block = block.block, api = client)
    }

    private suspend fun blockEvents(
        block: FlowBlock,
        api: AsyncFlowAccessApi
    ): Flow<FlowBlockchainLog> {
        if (block.collectionGuarantees.isEmpty()) {
            return emptyFlow()
        }

        val collections =
            block.collectionGuarantees.map { api.getCollectionById(it.id).asDeferred() }.awaitAll().filterNotNull()
        val results = collections.asFlow().flatMapMerge {
            it.transactionIds.asFlow().map { it to api.getTransactionResultById(it).asDeferred() }.buffer()
                .map {
                    it.first to it.second.await()!!
                }
        }

        return channelFlow {
            results.collect {
                val txId = it.first
                val res = it.second

                if (res.errorMessage.isNotEmpty()) {
                    send(
                        FlowBlockchainLog(
                            hash = txId.base16Value,
                            blockHash = block.id.base16Value,
                            event = null,
                            errorMessage = res.errorMessage
                        )
                    )
                } else {
                    res.events.forEach { flowEvent ->
                        send(
                            FlowBlockchainLog(
                                hash = txId.base16Value,
                                blockHash = block.id.base16Value,
                                event = flowEvent,
                                errorMessage = null
                            )
                        )
                    }
                }
            }
        }
    }
}
