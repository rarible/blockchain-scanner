package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.consumer.TransactionRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.TransactionRecordFilter
import com.rarible.blockchain.scanner.consumer.TransactionRecordMapper
import com.rarible.blockchain.scanner.framework.listener.TransactionRecordEventListener
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

class KafkaTransactionRecordConsumerWorkerFactory(
    host: String,
    environment: String,
    blockchain: String,
    private val service: String,
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
    private val meterRegistry: MeterRegistry,
) : TransactionRecordConsumerWorkerFactory {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain, "transaction")
    private val clientIdPrefix = "$environment.$host.${java.util.UUID.randomUUID()}.$blockchain"

    override fun <T> create(
        listener: TransactionRecordEventListener,
        transactionRecordType: Class<T>,
        transactionRecordMapper: TransactionRecordMapper<T>,
        logRecordFilters: List<TransactionRecordFilter<T>>,
        workerCount: Int,
    ): ConsumerWorkerHolder<T> {
        val workers = (1..workerCount).map { index ->
            val consumerGroup = listener.id
            val kafkaConsumer = RaribleKafkaConsumer(
                clientId = "$clientIdPrefix.transaction-event-consumer.$service.${listener.id}-$index",
                valueDeserializerClass = JsonDeserializer::class.java,
                valueClass = transactionRecordType,
                consumerGroup = consumerGroup,
                defaultTopic = "$topicPrefix.${listener.groupId}",
                bootstrapServers = properties.brokerReplicaSet,
                offsetResetStrategy = OffsetResetStrategy.EARLIEST,
                autoCreateTopic = false,
            )
            ConsumerBatchWorker(
                consumer = kafkaConsumer,
                properties = daemonProperties,
                // Block consumer should NOT skip events, so there is we're using endless retry
                retryProperties = RetryProperties(attempts = Integer.MAX_VALUE, delay = Duration.ofMillis(1000)),
                eventHandler = KafkaTransactionRecordEventHandler(listener, transactionRecordMapper, logRecordFilters),
                meterRegistry = meterRegistry,
                workerName = "transaction-event-consumer-${listener.id}-$index"
            )
        }
        return ConsumerWorkerHolder(workers)
    }
}