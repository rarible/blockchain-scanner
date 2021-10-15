package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@Component
class LastReadBlock(
    private val blockService: FlowBlockService,
    private val api: FlowGrpcApi
) {


    suspend fun getLastReadBlockHeight(): Long {
        val lastBlockInDb = blockService.getLastBlock()
        val lastBlockOnChain = api.latestBlock()

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
