package com.rarible.blockchain.scanner.ethereum.job

import com.rarible.blockchain.scanner.ethereum.service.EthereumPendingLogService
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration

@FlowPreview
@Component
@Profile("!test")
class EthereumExpiredPendingLogsCheckJob(
    private val ethereumPendingLogService: EthereumPendingLogService
) {

    private val logger = LoggerFactory.getLogger(EthereumExpiredPendingLogsCheckJob::class.java)

    @Autowired
    @Value("#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.maxPendingLogDuration}")
    private var maxPendingLogDuration: Long = 0

    @Scheduled(
        fixedRateString = "#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.interval}",
        initialDelayString = "#{blockchainScannerPropertiesProvider.properties.job.pendingLogs.initialDelay}"
    )
    fun job() {
        runBlocking {
            logger.info("Started job to drop expired pending logs")
            ethereumPendingLogService.dropExpiredPendingLogs(Duration.ofMillis(maxPendingLogDuration))
        }
    }
}
