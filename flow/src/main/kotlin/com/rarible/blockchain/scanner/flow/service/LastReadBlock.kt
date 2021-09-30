package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.FlowAccessApiClientManager
import kotlinx.coroutines.future.await
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@Component
class LastReadBlock(
    private val blockService: FlowBlockService,
    @Value("\${blockchain.scanner.flow.chainId}")
    private val chainId: FlowChainId = Flow.DEFAULT_CHAIN_ID,
) {

    suspend fun getLastReadBlockHeight(): Long {
        val api = FlowAccessApiClientManager.asyncForCurrentSpork(chainId)
        val lastBlockInDb = blockService.getLastBlock()
        val lastBlockOnChain = api.getLatestBlock(true).await()

        return if (lastBlockInDb != null) {
            val diff = ChronoUnit.SECONDS.between(
                Instant.ofEpochSecond(lastBlockInDb.timestamp), lastBlockOnChain.timestamp.toInstant(
                    ZoneOffset.UTC
                )
            )
            if (diff >= 60L) {
                lastBlockOnChain.height - 1
            } else {
                lastBlockInDb.id
            }
        } else {
            lastBlockOnChain.height - 1
        }
    }
}
