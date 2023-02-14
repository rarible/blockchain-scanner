package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.consumer.LogRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.LogRecordFilter
import com.rarible.blockchain.scanner.consumer.LogRecordMapper
import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.RetryProperties
import com.rarible.core.daemon.sequential.ConsumerBatchWorker
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder
import com.rarible.core.kafka.RaribleKafkaConsumer
import com.rarible.core.kafka.json.JsonDeserializer
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import java.time.Duration

class KafkaLogRecordConsumerWorkerFactory(
    host: String,
    environment: String,
    blockchain: String,
    private val service: String,
    private val workerCount: Int,
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
    private val meterRegistry: MeterRegistry,
) : LogRecordConsumerWorkerFactory {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain)
    private val clientIdPrefix = "$environment.$host.${java.util.UUID.randomUUID()}.$blockchain"

    override fun <T> create(
        listener: LogRecordEventListener,
        logRecordType: Class<T>,
        logRecordMapper: LogRecordMapper<T>,
        logRecordFilters: List<LogRecordFilter<T>>,
    ): ConsumerWorkerHolder<T> {
        val workers = (1..workerCount).map { index ->
            val consumerGroup = listener.id
            val kafkaConsumer = RaribleKafkaConsumer(
                clientId = "$clientIdPrefix.log-event-consumer.$service.${listener.id}-$index",
                valueDeserializerClass = JsonDeserializer::class.java,
                valueClass = logRecordType,
                consumerGroup = consumerGroup,
                defaultTopic = "$topicPrefix.${listener.groupId}",
                bootstrapServers = properties.brokerReplicaSet,
                offsetResetStrategy = OffsetResetStrategy.EARLIEST,
                autoCreateTopic = false
            )
            ConsumerBatchWorker(
                consumer = kafkaConsumer,
                properties = daemonProperties,
                // Block consumer should NOT skip events, so there is we're using endless retry
                retryProperties = RetryProperties(attempts = Integer.MAX_VALUE, delay = Duration.ofMillis(1000)),
                eventHandler = KafkaLogRecordEventHandler(listener, logRecordMapper, logRecordFilters),
                meterRegistry = meterRegistry,
                workerName = "log-event-consumer-${listener.id}-$index"
            )
        }
        return ConsumerWorkerHolder(workers)
    }
}