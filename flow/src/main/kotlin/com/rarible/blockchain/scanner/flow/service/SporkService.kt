package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.FlowId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class SporkService(
    properties: FlowBlockchainScannerProperties,
    private val flowApiFactory: FlowApiFactory,
) {
    private val sporks: List<Spork> = properties.sporks

    init {
        logger.info("Sporks: ${properties.sporks}")
    }

    fun sporks(range: LongRange): kotlinx.coroutines.flow.Flow<Spork> {
        val first = sporks.firstOrNull { it.containsBlock(range.first) }
        val second = sporks.firstOrNull { it.containsBlock(range.last) }
        return setOf(first, second).filterNotNull().asFlow()
    }

    fun spork(height: Long): Spork =
        sporks.first { it.containsBlock(height) }

    suspend fun spork(blockId: FlowId): Spork =
        sporks.asFlow().filter { containsBlock(it, blockId) }.first()

    suspend fun sporkForTx(txId: FlowId): Spork =
        sporks.asFlow().filter { containsTx(it, txId) }.first()

    fun currentSpork(): Spork = sporks.first()

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

    companion object {
        private val logger = LoggerFactory.getLogger(SporkService::class.java)
    }
}
