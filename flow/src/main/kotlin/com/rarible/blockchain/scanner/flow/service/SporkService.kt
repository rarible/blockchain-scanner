package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.flow.asFlow
import org.onflow.protobuf.access.AccessAPIGrpc
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class SporkService(
    @Value("\${blockchain.scanner.flow.chainId}")
    val chainId: FlowChainId
) {

    data class Spork(val from: Long, val to: Long = Long.MAX_VALUE, val nodeUrl: String, val port: Int = 9000) {

        @Suppress("PrivatePropertyName")
        private val DEFAULT_MESSAGE_SIZE: Int = 16777216 //16 Mb

        val api by lazy {
            val channel = ManagedChannelBuilder.forAddress(nodeUrl, port)
                .maxInboundMessageSize(DEFAULT_MESSAGE_SIZE)
                .usePlaintext()
                .userAgent(Flow.DEFAULT_USER_AGENT)
                .build()
            AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
        }

        fun containsBlock(blockHeight: Long): Boolean = blockHeight in from..to

        fun containsBlock(id: FlowId): Boolean =
            try {
                api.getBlockHeaderById(id).join() != null
            } catch (_: Exception) {
                false
            }

        fun containsTx(id: FlowId): Boolean =
            try {
                api.getTransactionById(id).join() != null
            } catch (_: Exception) {
                false
            }

        fun trim(range: LongRange): LongRange {
            val first = if (from < range.first) range.first else from
            val last = if (to < range.last) to else range.last
            return first..last
        }
    }

    val allSporks = mutableMapOf(
        FlowChainId.TESTNET to listOf(
            Spork(from = 50540411L, nodeUrl = "access.devnet.nodes.onflow.org"),
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
            Spork(from = 17544523L, to = 18587477L, nodeUrl = "access-001.mainnet12.nodes.onflow.org"),
            Spork(from = 18587478L, to = 19050752L, nodeUrl = "access-001.mainnet13.nodes.onflow.org"),
            Spork(from = 19050753L, nodeUrl = "access.mainnet.nodes.onflow.org"),
        ).reversed()
    )

    fun sporks(range: LongRange): kotlinx.coroutines.flow.Flow<Spork> {
        val first = allSporks[chainId]!!.firstOrNull { it.containsBlock(range.first) }
        val second = allSporks[chainId]!!.firstOrNull { it.containsBlock(range.last) }
        return setOf(first, second).filterNotNull().asFlow()
    }

    fun spork(height: Long): Spork =
        allSporks[chainId]!!.first { it.containsBlock(height) }

    fun spork(blockId: FlowId): Spork =
        allSporks[chainId]!!.first { it.containsBlock(blockId) }

    fun sporkForTx(txId: FlowId): Spork =
        allSporks[chainId]!!.first { it.containsTx(txId) }

    fun currentSpork(): Spork = allSporks[chainId]!!.first()
}
