package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.publisher.DefaultKafkaLogRecordEventWrapper
import com.rarible.blockchain.scanner.publisher.KafkaLogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.KafkaLogRecordEventWrapper
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
    @ConditionalOnMissingBean(KafkaLogRecordEventWrapper::class)
    fun kafkaLogRecordEventWrapper(): KafkaLogRecordEventWrapper<LogRecordEvent> =
        DefaultKafkaLogRecordEventWrapper()

    @Bean
    @ConditionalOnMissingBean(LogRecordEventPublisher::class)
    fun kafkaLogEventPublisher(kafkaLogRecordEventWrapper: KafkaLogRecordEventWrapper<*>): LogRecordEventPublisher {
        if (kafkaProperties.enabled) {
            logger.info(
                "Custom {} is not configured, using {} with {}",
                LogRecordEventPublisher::class.java.simpleName,
                KafkaLogRecordEventPublisher::class.java.name,
                kafkaLogRecordEventWrapper::class.java
            )
            return KafkaLogRecordEventPublisher(
                properties = kafkaProperties,
                environment = applicationEnvironmentInfo.name,
                blockchain = properties.blockchain,
                service = properties.service,
                kafkaLogRecordEventWrapper = kafkaLogRecordEventWrapper,
                numberOfPartitionsPerGroup = kafkaProperties.numberOfPartitionsPerLogGroup
            )
        } else {
            logger.info("Kafka topics for log records are disabled")
            return object : LogRecordEventPublisher {
                override suspend fun isEnabled() = false
                override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) = Unit
            }
        }
    }
}
