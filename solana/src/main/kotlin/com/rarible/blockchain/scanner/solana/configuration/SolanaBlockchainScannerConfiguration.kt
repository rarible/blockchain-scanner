package com.rarible.blockchain.scanner.solana.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.solana.client.SolanaApi
import com.rarible.blockchain.scanner.solana.client.SolanaClient
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import com.rarible.core.mongo.configuration.EnableRaribleMongo
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
}
