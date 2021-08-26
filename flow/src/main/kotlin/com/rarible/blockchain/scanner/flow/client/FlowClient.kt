package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowId
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.bouncycastle.util.encoders.Hex
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import javax.annotation.PreDestroy

/**
 * Client for Flow blockchain
 */
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@Component
class FlowClient(
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId = DEFAULT_CHAIN_ID,
    private var lastKnownBlockHeight: Long = 0L,
    private val poller: FlowNetNewBlockPoller
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    override fun listenNewBlocks(): Flow<FlowBlockchainBlock> = runBlocking {
        poller.poll(lastKnownBlockHeight).map { FlowBlockchainBlock(it) }
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val client = FlowAccessApiClientManager.async(number, chainId)
        val a = client.getBlockByHeight(number).join() ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a)
    }


    override suspend fun getBlock(hash: String): FlowBlockchainBlock {
        val client = FlowAccessApiClientManager.async(hash, chainId)
        val b = client.getBlockById(FlowId(hash)).join() ?: throw IllegalStateException("Block [$hash] not found!")
        return FlowBlockchainBlock(b)
    }

    override suspend fun getLastBlockNumber(): Long =
        FlowAccessApiClientManager.asyncForCurrentSpork(chainId).getLatestBlockHeader().join().height


    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        range: LongRange
    ): List<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> {
        val client = FlowAccessApiClientManager.async(range, chainId)

        return range.map {
            val block = checkNotNull(client.getBlockByHeight(it).await()) { "Unable to get block with height: $it" }
            val logs = blockEvents(block, client).toList()
            FullBlock(
                block = FlowBlockchainBlock(block),
                logs = logs
            )
        }
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val client = FlowAccessApiClientManager.asyncByTxHAsh(transactionHash, chainId)
        val tx = client.getTransactionById(FlowId(transactionHash)).join() ?: return null
        return TransactionMeta(hash = transactionHash, blockHash = Hex.toHexString(tx.referenceBlockId.bytes))
    }

    @PreDestroy
    fun stopListenNewBlocks() {
        poller.cancel()
    }

    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        block: FlowBlockchainBlock
    ): List<FlowBlockchainLog> {
        val client = FlowAccessApiClientManager.async(block.number, chainId)
        val chainBlock =
            checkNotNull(client.getBlockByHeight(block.number).await()) { "Unable to get block with height: " }
        return blockEvents(block = chainBlock, api = client).toList()
    }

    private suspend fun blockEvents(block: com.nftco.flow.sdk.FlowBlock, api: AsyncFlowAccessApi): Flow<FlowBlockchainLog> = channelFlow {
        block.collectionGuarantees.forEach { collectionGuarantee ->
            val collection = checkNotNull(
                api.getCollectionById(collectionGuarantee.id).await()
            ) { "Unable to get collection with id: ${collectionGuarantee.id.base16Value} in block with height: ${block.height}" }
            collection.transactionIds.forEach { txId ->
                val txResult = checkNotNull(
                    api.getTransactionResultById(txId).await()
                ) { "Unable to get transaction with id: ${txId.base16Value}, in collection with id: ${collectionGuarantee.id.base16Value}, in block with number: ${block.height}" }
                txResult.events.forEach {
                    send(FlowBlockchainLog(it))
                }
            }
        }
    }
}
