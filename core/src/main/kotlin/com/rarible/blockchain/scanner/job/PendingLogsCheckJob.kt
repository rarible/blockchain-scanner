package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.pending.PendingLogChecker
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class PendingLogsCheckJob(
    private val pendingLogChecker: PendingLogChecker
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogsCheckJob::class.java)

    @Scheduled(
        fixedRateString = "\${pendingLogsCheckJobInterval:${DateUtils.MILLIS_PER_HOUR}}",
        initialDelay = DateUtils.MILLIS_PER_MINUTE
    )
    fun job() {
        logger.info("Started job to check pending logs")
        pendingLogChecker.checkPendingLogs()
    }
}