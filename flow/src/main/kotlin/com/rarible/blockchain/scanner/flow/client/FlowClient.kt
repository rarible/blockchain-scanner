package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import org.bouncycastle.util.encoders.Hex
import org.onflow.sdk.AsyncFlowAccessApi
import org.onflow.sdk.FlowId
import org.onflow.sdk.Flow as FlowSDK

/**
 * Client for Flow blockchain
 */
@ExperimentalCoroutinesApi
class FlowClient(
    private val nodeUrl: String,
    private var lastKnownBlockHeight: Long,
    private val flowClient: AsyncFlowAccessApi = FlowSDK.newAsyncAccessApi(nodeUrl),
    dispatcher: CoroutineDispatcher = Dispatchers.Default
) : BlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor> {

    private  val poller = FlowNetNewBlockPoller(dispatcher = dispatcher, nodeUrl = nodeUrl)

    override fun listenNewBlocks(): Flow<FlowBlockchainBlock> {
        return poller.poll(lastKnownBlockHeight).map { FlowBlockchainBlock(it) }
    }

    override suspend fun getBlock(number: Long): FlowBlockchainBlock {
        val a = flowClient.getBlockByHeight(number).join() ?: throw IllegalStateException("Block [$number] not found!")
        return FlowBlockchainBlock(a)
    }


    override suspend fun getBlock(hash: String): FlowBlockchainBlock {
        val b = flowClient.getBlockById(FlowId(hash)).join() ?: throw IllegalStateException("Block [$hash] not found!")
        return FlowBlockchainBlock(b)
    }

    override suspend fun getLastBlockNumber(): Long =
        flowClient.getLatestBlockHeader().join().height


    override suspend fun getBlockEvents(
        block: FlowBlockchainBlock,
        descriptor: FlowDescriptor
    ): List<FlowBlockchainLog> {
        val results = flowClient.getEventsForBlockIds("", setOf(FlowId(block.hash))).join()
        return results.flatMap { r ->
            r.events.map { e ->
                FlowBlockchainLog(
                    blockId = r.blockId.base16Value,
                    txId = e.transactionId.base16Value
                )
            }
        }
    }

    override fun getBlockEvents(
        descriptor: FlowDescriptor,
        range: LongRange
    ): Flow<FullBlock<FlowBlockchainBlock, FlowBlockchainLog>> {
        val results = flowClient.getEventsForHeightRange("", range).join()
        return results.map { r ->
            val block = flowClient.getBlockByHeight(r.blockHeight).join()!!
            FullBlock(
                block = FlowBlockchainBlock(block),
                logs = r.events.map { FlowBlockchainLog(blockId = Hex.toHexString(block.id.bytes), txId = Hex.toHexString(it.transactionId.bytes)) }
            )
        }.asFlow()
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val tx = flowClient.getTransactionById(FlowId(transactionHash)).join() ?: return null
        return TransactionMeta(hash = transactionHash, blockHash = Hex.toHexString(tx.referenceBlockId.bytes))
    }

    fun stopListenNewBlocks() {
        poller.cancel()
    }
}
