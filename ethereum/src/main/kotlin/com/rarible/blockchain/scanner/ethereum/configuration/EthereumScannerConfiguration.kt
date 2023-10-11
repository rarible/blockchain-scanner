package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.reconciliation.EthereumLogReconciliationMonitor
import com.rarible.blockchain.scanner.ethereum.reconciliation.OnReconciliationListener
import com.rarible.blockchain.scanner.ethereum.reconciliation.ReconciliationLogHandler
import com.rarible.blockchain.scanner.ethereum.reconciliation.ReconciliationLogWorkerHandler
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.job.JobDaemonWorker
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@ExperimentalCoroutinesApi
@EnableBlockchainScanner
@Configuration
@EnableRaribleMongo
@EnableScaletherMongoConversions
@EnableConfigurationProperties(EthereumScannerProperties::class)
@ComponentScan(basePackageClasses = [EthereumScanner::class])
class EthereumScannerConfiguration(
    private val properties: EthereumScannerProperties,
    private val meterRegistry: MeterRegistry
) {

    @Bean
    @ConditionalOnProperty(
        name = [
            "blockchain.scanner.ethereum.reconciliation.enabled",
            "blockchain.scanner.ethereum.scan.enabled",
        ],
        havingValue = "true"
    )
    fun blockchainScannerReconciliationLogHandlerWorker(handler: ReconciliationLogWorkerHandler): JobDaemonWorker {
        return JobDaemonWorker(
            jobHandler = handler,
            meterRegistry = meterRegistry,
            properties = DaemonWorkerProperties(
                pollingPeriod = properties.reconciliation.checkPeriod,
            ),
            workerName = "blockchain-scanner-reconciliation-log-handler-worker"
        ).apply { start() }
    }

    @Bean
    fun reconciliationLogHandler(
        manager: EthereumScannerManager,
        logRepository: EthereumLogRepository,
        logService: EthereumLogService,
        onReconciliationListeners: List<OnReconciliationListener>,
        scannerProperties: EthereumScannerProperties,
        monitor: EthereumLogReconciliationMonitor,
        subscribers: List<EthereumLogEventSubscriber>,
    ): ReconciliationLogHandler {
        return ReconciliationLogHandler(
            logRepository = logRepository,
            logService = logService,
            onReconciliationListeners = onReconciliationListeners,
            scannerProperties = scannerProperties,
            ethereumClient = manager.retryableClient,
            logHandlerFactory = manager.logHandlerFactory,
            reindexer = manager.blockReindexer,
            planner = manager.blockScanPlanner,
            monitor = monitor,
            subscribers = subscribers
        )
    }
}
