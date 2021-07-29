package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.pending.PendingBlockChecker
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class PendingBlocksCheckJob(
    private val pendingBlockChecker: PendingBlockChecker
) {
    @Scheduled(
        fixedRateString = "\${pendingBlocksCheckJobInterval:${DateUtils.MILLIS_PER_MINUTE}}",
        initialDelayString = "\${pendingBlocksCheckJobInterval:${DateUtils.MILLIS_PER_MINUTE}}"
    )
    fun job() {
        logger.info("started")
        pendingBlockChecker.checkPendingBlocks()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingBlocksCheckJob::class.java)
    }
}