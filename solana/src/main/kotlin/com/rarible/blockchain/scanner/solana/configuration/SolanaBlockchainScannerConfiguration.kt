package com.rarible.blockchain.scanner.solana.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.reconciliation.LogReconciliationMonitor
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandler
import com.rarible.blockchain.scanner.reconciliation.ReconciliationLogHandlerImpl
import com.rarible.blockchain.scanner.solana.SolanaBlockchainScannerManager
import com.rarible.blockchain.scanner.solana.client.SolanaApi
import com.rarible.blockchain.scanner.solana.client.SolanaClient
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@Configuration
@EnableBlockchainScanner
@EnableRaribleMongo
@EnableMongock
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.solana"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.solana.repository"])
@EnableConfigurationProperties(SolanaBlockchainScannerProperties::class)
class SolanaBlockchainScannerConfiguration {
    @Bean
    fun client(
        api: SolanaApi,
        properties: SolanaBlockchainScannerProperties,
        subscribers: List<SolanaLogEventSubscriber>
    ): SolanaClient = SolanaClient(
        api = api,
        properties = properties,
        programIds = subscribers.map { it.getDescriptor().programId }.toSet()
    )

    @Bean
    @ConditionalOnProperty(
        name = [
            "blockchain.scanner.solana.reconciliation.reconciliation.enabled",
            "blockchain.scanner.solana.scan.scan.enabled",
        ],
        havingValue = "true"
    )
    fun reconciliationLogHandler(
        manager: SolanaBlockchainScannerManager,
        scannerProperties: SolanaBlockchainScannerProperties,
        monitor: LogReconciliationMonitor,
        subscribers: List<SolanaLogEventSubscriber>,
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
