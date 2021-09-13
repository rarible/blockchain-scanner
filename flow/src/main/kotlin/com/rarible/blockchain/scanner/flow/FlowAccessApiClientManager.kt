package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.AsyncFlowAccessApi
import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.Flow.DEFAULT_CHAIN_ID
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowId
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking

object FlowAccessApiClientManager {

    data class Spork(val from: Long, val to: Long = Long.MAX_VALUE, val nodeUrl: String, val port: Int = 9000) {

        val asyncClient by lazy { Flow.newAsyncAccessApi(nodeUrl, port) }

        fun containsBlock(blockHeight: Long): Boolean = blockHeight in from..to

        fun containsBlock(blockHash: String): Boolean = runBlocking(Dispatchers.IO) {
            try {
                asyncClient.getBlockHeaderById(FlowId(blockHash)).await() != null
            } catch (e: Exception) {
                false
            }
        }

        fun containsTransaction(txHash: String): Boolean = runBlocking(Dispatchers.IO) {
            try {
                asyncClient.getTransactionById(FlowId(txHash)).await() != null
            } catch (e: Exception) {
                false
            }

        }
    }

    val sporks = mutableMapOf(
        FlowChainId.TESTNET to listOf(
            Spork(from = 43212001L, nodeUrl = "access.devnet.nodes.onflow.org"),
        ),

        FlowChainId.MAINNET to listOf(
            Spork(from = 7601063L, to = 8742958L, nodeUrl = "access-001.mainnet1.nodes.onflow.org"),
            Spork(from = 8742959L, to = 9737132L, nodeUrl = "access-001.mainnet2.nodes.onflow.org"),
            Spork(from = 9737133L, to = 9992019L, nodeUrl = "access-001.mainnet3.nodes.onflow.org"),
            Spork(from = 9992020L, to = 12020336L, nodeUrl = "access-001.mainnet4.nodes.onflow.org"),
            Spork(from = 12020337L, to = 12609236L, nodeUrl = "access-001.mainnet5.nodes.onflow.org"),
            Spork(from = 12609237L, to = 13404173L, nodeUrl = "access-001.mainnet6.nodes.onflow.org"),
            Spork(from = 13404174L, to = 13950741L, nodeUrl = "access-001.mainnet7.nodes.onflow.org"),
            Spork(from = 13950742L, to = 14892103L, nodeUrl = "access-001.mainnet8.nodes.onflow.org"),
            Spork(from = 14892104L, to = 15791890L, nodeUrl = "access-001.mainnet9.nodes.onflow.org"),
            Spork(from = 15791891L, to = 16755601L, nodeUrl = "access-001.mainnet10.nodes.onflow.org"),
            Spork(from = 16755602L, to = 17544522L, nodeUrl = "access-001.mainnet11.nodes.onflow.org"),
            Spork(from = 17544523L, nodeUrl = "access.mainnet.nodes.onflow.org"),
        )
    )

    fun async(blockHeight: Long, chainId: FlowChainId = DEFAULT_CHAIN_ID): Spork =
        sporkByBlockHeight(blockHeight, chainId)

    fun async(range: LongRange, chainId: FlowChainId = DEFAULT_CHAIN_ID): List<Spork> {
        val start = sporkByBlockHeight(range.first, chainId)
        val end = sporkByBlockHeight(range.last, chainId)

        return if (start == end) {
            listOf(start)
        } else {
            listOf(start, end)
        }
    }

    /**
     * This is bottle-neck!!!
     */
    fun async(hash: String, chainId: FlowChainId = DEFAULT_CHAIN_ID): Spork =
        sporks[chainId]!!.first { it.containsBlock(blockHash = hash) }

    fun asyncByTxHash(transactionHash: String, chainId: FlowChainId): Spork {
        return sporks[chainId]!!.first { it.containsTransaction(transactionHash) }
    }

    fun asyncForCurrentSpork(chainId: FlowChainId): AsyncFlowAccessApi = sporks[chainId]!!.last().asyncClient

    private fun sporkByBlockHeight(blockHeight: Long, chainId: FlowChainId): Spork =
        sporks[chainId]?.find { blockHeight in it.from..it.to }
            ?: throw IllegalArgumentException("Not found spork for blockHeight = $blockHeight in ${chainId.id}")

}
