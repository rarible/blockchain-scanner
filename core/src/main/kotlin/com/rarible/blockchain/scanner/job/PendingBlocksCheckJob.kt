package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.pending.PendingBlockChecker
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class PendingBlocksCheckJob(
    private val pendingBlockChecker: PendingBlockChecker,
    private val properties: BlockchainScannerProperties
) {

    private val logger = LoggerFactory.getLogger(PendingBlocksCheckJob::class.java)

    @Scheduled(
        fixedRateString = "#{blockchainScannerPropertiesProvider.properties.job.pendingBlocks.interval}",
        initialDelayString = "#{blockchainScannerPropertiesProvider.properties.job.pendingBlocks.initialDelay}"
    )
    fun job() {
        logger.info("Started job to check pending blocks")
        pendingBlockChecker.checkPendingBlocks(properties.job.pendingBlocks.pendingBlockAgeToCheck)
    }
}