package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.FlowChainId
import com.nftco.flow.sdk.FlowId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.future.await
import org.springframework.stereotype.Service

@Service
class SporkService(
    properties: FlowBlockchainScannerProperties,
    private val flowApiFactory: FlowApiFactory,
) {
    private val sporksMap = mutableMapOf(
        FlowChainId.TESTNET to listOf(
            Spork(
                from = 50540412L,
                nodeUrl = "access.devnet.nodes.onflow.org",
            ),
        ),

        FlowChainId.MAINNET to listOf(
            Spork(
                from = 7601063L,
                to = 8742958L,
                nodeUrl = "access-001.mainnet1.nodes.onflow.org",
            ),
            Spork(
                from = 8742959L,
                to = 9737132L,
                nodeUrl = "access-001.mainnet2.nodes.onflow.org",
            ),
            Spork(
                from = 9737133L,
                to = 9992019L,
                nodeUrl = "access-001.mainnet3.nodes.onflow.org",
            ),
            Spork(
                from = 9992020L,
                to = 12020336L,
                nodeUrl = "access-001.mainnet4.nodes.onflow.org",
            ),
            Spork(
                from = 12020337L,
                to = 12609236L,
                nodeUrl = "access-001.mainnet5.nodes.onflow.org",
            ),
            Spork(
                from = 12609237L,
                to = 13404173L,
                nodeUrl = "access-001.mainnet6.nodes.onflow.org",
            ),
            Spork(
                from = 13404174L,
                to = 13950741L,
                nodeUrl = "access-001.mainnet7.nodes.onflow.org",
            ),
            Spork(
                from = 13950742L,
                to = 14892103L,
                nodeUrl = "access-001.mainnet8.nodes.onflow.org",
            ),
            Spork(
                from = 14892104L,
                to = 15791890L,
                nodeUrl = "access-001.mainnet9.nodes.onflow.org",
            ),
            Spork(
                from = 15791891L,
                to = 16755601L,
                nodeUrl = "access-001.mainnet10.nodes.onflow.org",
            ),
            Spork(
                from = 16755602L,
                to = 17544522L,
                nodeUrl = "access-001.mainnet11.nodes.onflow.org",
            ),
            Spork(
                from = 17544523L,
                to = 18587477L,
                nodeUrl = "access-001.mainnet12.nodes.onflow.org",
            ),
            Spork(
                from = 18587478L,
                to = 19050752L,
                nodeUrl = "access-001.mainnet13.nodes.onflow.org",
            ),
            Spork(
                from = 19050753L,
                nodeUrl = properties.nodeUrl,
            ),
        ).reversed()
    )

    val chainId = properties.chainId

    fun sporks(chainId: FlowChainId): List<Spork> = sporksMap[chainId] ?: emptyList()

    fun replace(chainId: FlowChainId, sporks: List<Spork>) {
        sporksMap[chainId] = sporks
    }

    fun sporks(range: LongRange): kotlinx.coroutines.flow.Flow<Spork> {
        val first = sporksMap[chainId]!!.firstOrNull { it.containsBlock(range.first) }
        val second = sporksMap[chainId]!!.firstOrNull { it.containsBlock(range.last) }
        return setOf(first, second).filterNotNull().asFlow()
    }

    fun spork(height: Long): Spork =
        sporksMap[chainId]!!.first { it.containsBlock(height) }

    suspend fun spork(blockId: FlowId): Spork =
        sporksMap[chainId]!!.asFlow().filter { containsBlock(it, blockId) }.first()

    suspend fun sporkForTx(txId: FlowId): Spork =
        sporksMap[chainId]!!.asFlow().filter { containsTx(it, txId) }.first()

    fun currentSpork(): Spork = sporksMap[chainId]!!.first()

    private suspend fun containsBlock(spork: Spork, id: FlowId): Boolean =
        try {
            flowApiFactory.getApi(spork).getBlockHeaderById(id).await() != null
        } catch (_: Exception) {
            false
        }

    private suspend fun containsTx(spork: Spork, id: FlowId): Boolean =
        try {
            flowApiFactory.getApi(spork).getTransactionById(id).await() != null
        } catch (_: Exception) {
            false
        }
}
