package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import com.rarible.blockchain.scanner.ethereum.EthereumScannerManager
import com.rarible.blockchain.scanner.ethereum.client.EthereumClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumClientFactory
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriber
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumTransactionEventSubscriber
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.monitoring.ReindexMonitor
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
    fun ethereumClient(ethereumClientFactory: EthereumClientFactory): EthereumClient =
        ethereumClientFactory.createMainClient()

    @Bean
    fun reconciliationManager(
        blockchainClientFactory: EthereumClientFactory,
        subscribers: List<EthereumLogEventSubscriber>,
        blockService: BlockService,
        logService: EthereumLogService,
        properties: BlockchainScannerProperties,
        logRecordEventPublisher: LogRecordEventPublisher,
        blockMonitor: BlockMonitor,
        logMonitor: LogMonitor,
        reindexMonitor: ReindexMonitor,
        transactionSubscribers: List<EthereumTransactionEventSubscriber>,
        transactionRecordEventPublisher: TransactionRecordEventPublisher,
        logEventSubscriberExceptionResolver: EthereumLogEventSubscriberExceptionResolver,
    ): EthereumScannerManager {
        val reconciliationClient = blockchainClientFactory.createReconciliationClient()
        return EthereumScannerManager(
            ethereumClient = reconciliationClient,
            subscribers = subscribers,
            blockService = blockService,
            logService = logService,
            properties = properties,
            logRecordEventPublisher = logRecordEventPublisher,
            blockMonitor = blockMonitor,
            logMonitor = logMonitor,
            reindexMonitor = reindexMonitor,
            transactionSubscribers = transactionSubscribers,
            transactionRecordEventPublisher = transactionRecordEventPublisher,
            logEventSubscriberExceptionResolver = logEventSubscriberExceptionResolver,
        )
    }
}
