package com.rarible.blockchain.scanner.ethereum.consumer.factory

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.ethereum.consumer.handler.BlockEventHandler
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecordEvent
import com.rarible.blockchain.scanner.ethereum.reduce.EntityEventListener
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.RetryProperties
import com.rarible.core.daemon.sequential.ConsumerBatchWorker
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder
import com.rarible.core.kafka.RaribleKafkaConsumer
import com.rarible.core.kafka.json.JsonDeserializer
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import scalether.domain.Address
import java.time.Duration

class DefaultConsumerWorkerFactory(
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
    private val meterRegistry: MeterRegistry,
    private val ignoreContracts: Set<Address>,
    host: String,
    environment: String,
    blockchain: String,
    private val service: String,
    private val workerCount: Int
) : ConsumerWorkerFactory {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain)
    private val clientIdPrefix = "$environment.$host.${java.util.UUID.randomUUID()}.$blockchain"

    override fun create(listener: EntityEventListener): ConsumerWorkerHolder<EthereumLogRecordEvent> {
        val workers = (1..workerCount).map { index ->
            val consumerGroup = listener.id
            val kafkaConsumer = RaribleKafkaConsumer(
                clientId = "$clientIdPrefix.log-event-consumer.$service.${listener.id}-$index",
                valueDeserializerClass = JsonDeserializer::class.java,
                valueClass = EthereumLogRecordEvent::class.java,
                consumerGroup = consumerGroup,
                defaultTopic = "$topicPrefix.${listener.subscriberGroup}",
                bootstrapServers = properties.brokerReplicaSet,
                offsetResetStrategy = OffsetResetStrategy.EARLIEST,
                autoCreateTopic = false
            )
            ConsumerBatchWorker(
                consumer = kafkaConsumer,
                properties = daemonProperties,
                // Block consumer should NOT skip events, so there is we're using endless retry
                retryProperties = RetryProperties(attempts = Integer.MAX_VALUE, delay = Duration.ofMillis(1000)),
                eventHandler = BlockEventHandler(listener, ignoreContracts),
                meterRegistry = meterRegistry,
                workerName = "log-event-consumer-${listener.id}-$index"
            )
        }
        return ConsumerWorkerHolder(workers)
    }
}