package com.rarible.blockchain.scanner.hedera.configuration

import com.github.cloudyrock.spring.v5.EnableMongock
import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.hedera.client.rest.HederaClientMetrics
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.HederaWebClientCustomizer
import com.rarible.blockchain.scanner.hedera.client.rest.WebClientFactory
import com.rarible.core.mongo.configuration.EnableRaribleMongo
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClient

@Configuration
@EnableBlockchainScanner
@EnableRaribleMongo
@EnableMongock
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.hedera"])
@EnableConfigurationProperties(HederaBlockchainScannerProperties::class)
class HederaBlockchainScannerConfiguration(
    private val properties: HederaBlockchainScannerProperties
) {
    @Bean
    fun hederaMirrorNodeClientProperties(): MirrorNodeClientProperties {
        return properties.mirrorNode
    }

    @Bean
    fun hederaWebClientCustomizer(
        hederaMirrorNodeClientProperties: MirrorNodeClientProperties
    ): HederaWebClientCustomizer {
        return HederaWebClientCustomizer(hederaMirrorNodeClientProperties)
    }

    @Bean
    fun hederaWebClient(
        hederaMirrorNodeClientProperties: MirrorNodeClientProperties,
        hederaWebClientCustomizer: HederaWebClientCustomizer
    ): WebClient {
        val webClientBuilder = WebClientFactory.createClient(
            hederaMirrorNodeClientProperties.endpoint,
            hederaMirrorNodeClientProperties.mirrorNodeHeaders
        )
        hederaWebClientCustomizer.customize(webClientBuilder)
        return webClientBuilder.build()
    }

    @Bean
    fun hederaClientMetrics(
        meterRegistry: MeterRegistry
    ): HederaClientMetrics {
        return HederaClientMetrics(meterRegistry)
    }

    @Bean
    fun hederaRestApiWebClients(
        hederaMirrorNodeClientProperties: MirrorNodeClientProperties,
        hederaClientMetrics: HederaClientMetrics,
        hederaWebClient: WebClient,
    ): HederaRestApiClient {
        return HederaRestApiClient(
            webClient = hederaWebClient,
            metrics = hederaClientMetrics,
            maxErrorBodyLogLength = hederaMirrorNodeClientProperties.maxErrorBodyLogLength,
            slowRequestLatency = hederaMirrorNodeClientProperties.slowRequestLatency
        )
    }
}
