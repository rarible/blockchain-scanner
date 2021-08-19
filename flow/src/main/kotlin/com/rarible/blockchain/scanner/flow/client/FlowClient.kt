package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.bouncycastle.util.encoders.Hex
import org.onflow.sdk.Flow.DEFAULT_CHAIN_ID
import org.onflow.sdk.FlowChainId
import org.onflow.sdk.FlowId

/**
 * Client for Flow blockchain
 */
@ExperimentalCoroutinesApi
class FlowClient(
    private val chainId: FlowChainId = DEFAULT_CHAIN_ID,
    private var lastKnownBlockHeight: Long,
    dispatcher: CoroutineDispatcher = Dispatchers.Default
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private  val poller = FlowNetNewBlockPoller(dispatcher = dispatcher, chainId = chainId)

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
        val results = client.getEventsForHeightRange("", range).join()
        return results.map { r ->
            val block = client.getBlockByHeight(r.blockHeight).join()!!
            FullBlock(
                block = FlowBlockchainBlock(block),
                logs = r.events.map { FlowBlockchainLog(it) }
            )
        }.toList()
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val client = FlowAccessApiClientManager.asyncByTxHAsh(transactionHash, chainId)
        val tx = client.getTransactionById(FlowId(transactionHash)).join() ?: return null
        return TransactionMeta(hash = transactionHash, blockHash = Hex.toHexString(tx.referenceBlockId.bytes))
    }

    fun stopListenNewBlocks() {
        poller.cancel()
    }

    override suspend fun getBlockEvents(
        descriptor: FlowDescriptor,
        block: FlowBlockchainBlock
    ): List<FlowBlockchainLog> {
        val client = FlowAccessApiClientManager.async(block.number, chainId)
        val events = client.getEventsForBlockIds("", setOf(FlowId(block.hash))).join()
        return events.first().events.map {
            FlowBlockchainLog(it)
        }
    }
}
