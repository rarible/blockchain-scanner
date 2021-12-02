package com.rarible.blockchain.scanner.ethereum.job

import com.rarible.blockchain.scanner.ethereum.pending.PendingLogChecker
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class EthereumPendingLogsCheckJob(
    private val pendingLogChecker: PendingLogChecker
) {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogsCheckJob::class.java)

    @Scheduled(
        fixedRateString = "#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.interval}",
        initialDelayString = "#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.initialDelay}"
    )
    fun job() {
        runBlocking {
            logger.info("Started job to check pending logs")
            pendingLogChecker.checkPendingLogs()
        }
    }
}