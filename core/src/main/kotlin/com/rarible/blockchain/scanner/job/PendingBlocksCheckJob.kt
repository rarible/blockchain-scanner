package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.BlockchainScannerService
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import kotlin.math.abs

@Service
class PendingBlocksCheckJob<B : Block>(
    private val blockService: BlockService<B>,
    private val blockchainScannerService: BlockchainScannerService<*, *, B, *, *>
) {
    @Scheduled(
        fixedRateString = "\${pendingBlocksCheckJobInterval:${DateUtils.MILLIS_PER_MINUTE}}",
        initialDelayString = "\${pendingBlocksCheckJobInterval:${DateUtils.MILLIS_PER_MINUTE}}"
    )
    fun job() {
        logger.info("started")
        try {
            Flux.concat(
                blockService.findByStatus(Block.Status.PENDING).filter {
                    abs(System.currentTimeMillis() / 1000 - it.timestamp) > 60
                },
                blockService.findByStatus(Block.Status.ERROR)
            )
                .concatMap { blockchainScannerService.reindexPendingBlock(it) }
                .then().block()
            logger.info("ended")
        } catch (e: Exception) {
            logger.error("error", e)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingBlocksCheckJob::class.java)
    }
}