package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.reconciliation.LogReconciliationMonitor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandler
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandlerImpl
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
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
class EthereumScannerConfiguration {

    @Bean
    @ConditionalOnProperty(
        name = [
            "blockchain.scanner.ethereum.reconciliation.enabled",
            "blockchain.scanner.ethereum.scan.enabled",
        ],
        havingValue = "true"
    )
    fun reconciliationLogHandler(
        manager: EthereumScannerManager,
        logService: EthereumLogService,
        scannerProperties: EthereumScannerProperties,
        monitor: LogReconciliationMonitor,
        subscribers: List<EthereumLogEventSubscriber>,
    ): ReconciliationLogHandler {
        return ReconciliationLogHandlerImpl(
            logService = logService,
            reconciliationProperties = scannerProperties.reconciliation,
            blockchainClient = manager.retryableClient,
            logHandlerFactory = manager.logHandlerFactory,
            reindexer = manager.blockReindexer,
            planner = manager.blockScanPlanner,
            monitor = monitor,
            subscribers = subscribers,
        )
    }
}
