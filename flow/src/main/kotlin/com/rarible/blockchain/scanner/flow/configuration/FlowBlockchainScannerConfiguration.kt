package com.rarible.blockchain.scanner.flow.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.flow.FlowBlockchainScannerManager
import com.rarible.blockchain.scanner.flow.service.FlowLogService
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.reconciliation.LogReconciliationMonitor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandler
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandlerImpl
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@EnableBlockchainScanner
@Configuration
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.flow"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.flow.repository"])
@EnableConfigurationProperties(FlowBlockchainScannerProperties::class)
class FlowBlockchainScannerConfiguration {
    @Bean
    @ConditionalOnProperty(
        name = [
            "blockchain.scanner.flow.reconciliation.enabled",
            "blockchain.scanner.flow.scan.enabled",
        ],
        havingValue = "true"
    )
    fun reconciliationLogHandler(
        manager: FlowBlockchainScannerManager,
        logService: FlowLogService,
        scannerProperties: FlowBlockchainScannerProperties,
        monitor: LogReconciliationMonitor,
        subscribers: List<FlowLogEventSubscriber>,
    ): ReconciliationLogHandler {
        return ReconciliationLogHandlerImpl(
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
