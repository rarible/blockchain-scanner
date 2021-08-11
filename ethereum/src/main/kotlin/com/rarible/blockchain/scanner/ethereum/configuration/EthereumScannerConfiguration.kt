package com.rarible.blockchain.scanner.ethereum.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration


@Configuration
@EnableRaribleMongo
@EnableMongock
@EnableScaletherMongoConversions
@EnableConfigurationProperties(EthereumScannerProperties::class)
@ComponentScan(basePackageClasses = [EthereumScanner::class])
class EthereumScannerConfiguration