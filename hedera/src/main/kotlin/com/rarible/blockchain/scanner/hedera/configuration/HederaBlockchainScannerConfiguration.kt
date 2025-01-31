package com.rarible.blockchain.scanner.hedera.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@EnableBlockchainScanner
@EnableRaribleMongo
@EnableMongock
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.hedera"])
@EnableConfigurationProperties(HederaBlockchainScannerProperties::class)
class HederaBlockchainScannerConfiguration
