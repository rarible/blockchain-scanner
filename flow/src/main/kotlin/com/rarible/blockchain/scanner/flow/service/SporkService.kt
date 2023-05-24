package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import kotlinx.coroutines.flow.asFlow
import org.springframework.stereotype.Service

@Service
class SporkService(
    properties: FlowBlockchainScannerProperties,
) {
    private val sporksMap = mutableMapOf(
        FlowChainId.TESTNET to listOf(
            Spork(from = 50540412L, nodeUrl = "access.devnet.nodes.onflow.org"),
        ).withProxy(),

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
        ).withProxy().reversed()
    )

    val chainId = properties.chainId

    fun sporks(chainId: FlowChainId): List<Spork> = sporksMap[chainId] ?: emptyList()

    fun replace(chainId: FlowChainId, sporks: List<Spork>) {
        sporksMap[chainId] = sporks.withProxy()
    }

    fun sporks(range: LongRange): kotlinx.coroutines.flow.Flow<Spork> {
        val first = sporksMap[chainId]!!.firstOrNull { it.containsBlock(range.first) }
        val second = sporksMap[chainId]!!.firstOrNull { it.containsBlock(range.last) }
        return setOf(first, second).filterNotNull().asFlow()
    }

    fun spork(height: Long): Spork =
        sporksMap[chainId]!!.first { it.containsBlock(height) }

    fun spork(blockId: FlowId): Spork =
        sporksMap[chainId]!!.first { it.containsBlock(blockId) }

    fun sporkForTx(txId: FlowId): Spork =
        sporksMap[chainId]!!.first { it.containsTx(txId) }

    fun currentSpork(): Spork = sporksMap[chainId]!!.first()

    private fun List<Spork>.withProxy(): List<Spork> {
        return map { it.withProxy(null) }
    }
}

