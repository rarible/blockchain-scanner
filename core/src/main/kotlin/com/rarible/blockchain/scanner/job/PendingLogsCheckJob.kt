package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.pending.PendingLogChecker
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
@ConditionalOnBean(PendingLogChecker::class)
class PendingLogsCheckJob(
    private val pendingLogChecker: PendingLogChecker
) {

    private val logger = LoggerFactory.getLogger(PendingLogsCheckJob::class.java)

    @Scheduled(
        fixedRateString = "#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.interval}",
        initialDelayString = "#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.initialDelay}"
    )
    fun job() {
        logger.info("Started job to check pending logs")
        pendingLogChecker.checkPendingLogs()
    }
}