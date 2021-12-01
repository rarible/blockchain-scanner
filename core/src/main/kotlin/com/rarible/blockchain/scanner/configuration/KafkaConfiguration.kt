package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.consumer.BlockEventConsumer
import com.rarible.blockchain.scanner.consumer.KafkaBlockEventConsumer
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.publisher.KafkaBlockEventPublisher
import com.rarible.core.application.ApplicationEnvironmentInfo
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnProperty(name = ["blockchain.scanner.kafka.enabled"], havingValue = "true", matchIfMissing = false)
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfiguration(
    private val properties: BlockchainScannerProperties,
    private val kafkaProperties: KafkaProperties,
    private val applicationEnvironmentInfo: ApplicationEnvironmentInfo,
    private val meterRegistry: MeterRegistry
) {

    @Bean
    fun kafkaBlockEventConsumer(): BlockEventConsumer {
        return KafkaBlockEventConsumer(
            properties = kafkaProperties,
            daemonProperties = properties.daemon,
            meterRegistry = meterRegistry,
            host = applicationEnvironmentInfo.host,
            environment = applicationEnvironmentInfo.name,
            blockchain = properties.blockchain,
            service = properties.service
        )
    }

    @Bean
    fun kafkaBlockEventPublisher(): BlockEventPublisher {
        return KafkaBlockEventPublisher(
            properties = kafkaProperties,
            environment = applicationEnvironmentInfo.name,
            blockchain = properties.blockchain,
            service = properties.service
        )
    }

}