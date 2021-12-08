package com.rarible.blockchain.scanner.ethereum.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import com.rarible.blockchain.scanner.ethereum.subscriber.DefaultEthereumLogEventComparator
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumLogEventComparator
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.ethereum.converters.EnableScaletherMongoConversions
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration


@ExperimentalCoroutinesApi
@FlowPreview
@Configuration
@EnableRaribleMongo
@EnableMongock
@EnableScaletherMongoConversions
@EnableConfigurationProperties(EthereumScannerProperties::class)
@ComponentScan(basePackageClasses = [EthereumScanner::class])
class EthereumScannerConfiguration {

    @Bean
    @ConditionalOnMissingBean(EthereumLogEventComparator::class)
    fun logRecordComparator(): EthereumLogEventComparator {
        return DefaultEthereumLogEventComparator()
    }

}