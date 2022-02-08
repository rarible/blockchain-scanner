package com.rarible.blockchain.scanner.solana.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.solana.client.SolanaClient
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@Configuration
@EnableBlockchainScanner
@EnableRaribleMongo
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.solana"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.solana.repository"])
@EnableConfigurationProperties(SolanaBlockchainScannerProperties::class)
class SolanaBlockchainScannerConfiguration {
    @Bean
    fun client(properties: SolanaBlockchainScannerProperties): SolanaClient = SolanaClient(properties.rpcApiUrls)
}
