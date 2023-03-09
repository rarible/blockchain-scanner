package com.rarible.blockchain.scanner.flow.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@EnableBlockchainScanner
@Configuration
@EnableRaribleMongo
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.flow"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.flow.repository"])
@EnableConfigurationProperties(FlowBlockchainScannerProperties::class)
class FlowBlockchainScannerConfiguration
