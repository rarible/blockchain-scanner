package com.rarible.blockchain.scanner.ethereum.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.configuration.BlockchainScannerConfiguration
import com.rarible.blockchain.scanner.ethereum.EthereumBlockchainScanner
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import reactor.util.retry.Retry
import reactor.util.retry.RetryBackoffSpec
import java.time.Duration


@Configuration
@EnableRaribleMongo
@EnableMongock
@EnableScaletherMongoConversions
@EnableConfigurationProperties(EthereumBlockchainScannerProperties::class)
@ComponentScan(basePackageClasses = [EthereumBlockchainScanner::class])
@Import(BlockchainScannerConfiguration::class)
class EthereumScannerConfiguration(
    val properties: EthereumBlockchainScannerProperties
) {

    @Bean
    fun retrySpec(): RetryBackoffSpec {
        return Retry.backoff(properties.maxAttempts, Duration.ofMillis(properties.minBackoff))
    }

}