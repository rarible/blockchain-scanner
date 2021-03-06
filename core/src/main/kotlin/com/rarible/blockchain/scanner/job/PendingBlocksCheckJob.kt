package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.pending.PendingBlockChecker
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
@ConditionalOnBean(PendingBlockChecker::class)
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
        runBlocking {
            logger.info("Started job to check pending blocks")
            pendingBlockChecker.checkPendingBlocks(properties.job.pendingBlocks.pendingBlockAgeToCheck)
        }
    }
}