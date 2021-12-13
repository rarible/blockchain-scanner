package com.rarible.blockchain.scanner.solana.configuration

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@Configuration
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.solana"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.solana.repository"])
@EnableConfigurationProperties(SolanaBlockchainScannerProperties::class)
class SolanaBlockchainScannerConfiguration
