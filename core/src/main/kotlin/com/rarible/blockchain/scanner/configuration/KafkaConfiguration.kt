package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.blockchain.scanner.publisher.DefaultKafkaLogRecordEventWrapper
import com.rarible.blockchain.scanner.publisher.DefaultKafkaTransactionRecordEventWrapper
import com.rarible.blockchain.scanner.publisher.KafkaLogRecordEventWrapper
import com.rarible.blockchain.scanner.publisher.KafkaRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.KafkaTransactionRecordEventWrapper
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.publisher.RecordEventPublisher
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
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
    @ConditionalOnMissingBean(KafkaTransactionRecordEventWrapper::class)
    fun kafkaTransactionRecordEventWrapper(): KafkaTransactionRecordEventWrapper<TransactionRecordEvent> =
        DefaultKafkaTransactionRecordEventWrapper()

    @Bean
    @ConditionalOnMissingBean(LogRecordEventPublisher::class)
    fun kafkaLogEventPublisher(
        kafkaRecordEventWrapper: KafkaLogRecordEventWrapper<*>
    ): LogRecordEventPublisher {
        if (kafkaProperties.enabled) {
            logger.info(
                "Custom {} is not configured, using {} with {}",
                LogRecordEventPublisher::class.java.simpleName,
                KafkaRecordEventPublisher::class.java.name,
                kafkaRecordEventWrapper::class.java
            )
            val publisher = KafkaRecordEventPublisher(
                properties = kafkaProperties,
                environment = applicationEnvironmentInfo.name,
                blockchain = properties.blockchain,
                service = properties.service,
                type = "log",
                kafkaRecordEventWrapper = kafkaRecordEventWrapper,
                numberOfPartitionsPerGroup = kafkaProperties.numberOfPartitionsPerLogGroup
            )
            return object : LogRecordEventPublisher {
                override suspend fun isEnabled() = publisher.isEnabled()
                override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) =
                    publisher.publish(groupId, logRecordEvents)
            }
        } else {
            logger.info("Kafka topics for log records are disabled")
            return object : LogRecordEventPublisher {
                override suspend fun isEnabled() = false
                override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) = Unit
            }
        }
    }

    @Bean
    @ConditionalOnMissingBean(name = ["kafkaTransactionEventPublisher"])
    fun kafkaTransactionEventPublisher(
        kafkaRecordEventWrapper: KafkaTransactionRecordEventWrapper<*>
    ): TransactionRecordEventPublisher {
        if (kafkaProperties.enabled) {
            logger.info(
                "Custom {} is not configured, using {} with {}",
                RecordEventPublisher::class.java.simpleName,
                KafkaRecordEventPublisher::class.java.name,
                kafkaRecordEventWrapper::class.java
            )
            val publisher = KafkaRecordEventPublisher(
                properties = kafkaProperties,
                environment = applicationEnvironmentInfo.name,
                blockchain = properties.blockchain,
                service = properties.service,
                type = "transaction",
                kafkaRecordEventWrapper = kafkaRecordEventWrapper,
                numberOfPartitionsPerGroup = kafkaProperties.numberOfPartitionsPerLogGroup
            )
            return object : TransactionRecordEventPublisher {
                override suspend fun isEnabled() = publisher.isEnabled()
                override suspend fun publish(groupId: String, transactionRecordEvents: List<TransactionRecordEvent>) =
                    publisher.publish(groupId, transactionRecordEvents)
            }
        } else {
            logger.info("Kafka topics for transaction records are disabled")
            return object : TransactionRecordEventPublisher {
                override suspend fun isEnabled() = false
                override suspend fun publish(groupId: String, logRecordEvents: List<TransactionRecordEvent>) = Unit
            }
        }
    }
}
