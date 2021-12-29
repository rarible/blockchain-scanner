package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.publisher.KafkaLogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.core.application.ApplicationEnvironmentInfo
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfiguration(
    private val properties: BlockchainScannerProperties,
    private val kafkaProperties: KafkaProperties,
    private val applicationEnvironmentInfo: ApplicationEnvironmentInfo
) {

    private val logger = LoggerFactory.getLogger(KafkaConfiguration::class.java)

    @Bean
    @ConditionalOnMissingBean(LogRecordEventPublisher::class)
    fun kafkaLogEventPublisher(): LogRecordEventPublisher {
        logger.info(
            "Custom {} is not configured, using {}",
            LogRecordEventPublisher::class.java.simpleName,
            KafkaLogRecordEventPublisher::class.java.name
        )
        return KafkaLogRecordEventPublisher(
            properties = kafkaProperties,
            environment = applicationEnvironmentInfo.name,
            blockchain = properties.blockchain,
            service = properties.service
        )
    }
}
