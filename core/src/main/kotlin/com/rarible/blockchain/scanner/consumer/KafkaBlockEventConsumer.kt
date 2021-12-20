package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.util.getBlockTopic
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.RetryProperties
import com.rarible.core.daemon.sequential.ConsumerBatchEventHandler
import com.rarible.core.daemon.sequential.ConsumerBatchWorker
import com.rarible.core.kafka.RaribleKafkaConsumer
import com.rarible.core.kafka.json.JsonDeserializer
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import java.time.Duration

class KafkaBlockEventConsumer(
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
    private val meterRegistry: MeterRegistry,
    host: String,
    environment: String,
    blockchain: String,
    private val service: String
) : BlockEventConsumer {

    private val topic = getBlockTopic(environment, service, blockchain)
    private val clientIdPrefix = "$environment.$host.${java.util.UUID.randomUUID()}.$blockchain"

    override suspend fun start(handler: Map<String, BlockListener>) {
        handler
            .map { consumer(it.key, it.value) }
            .forEach { it.start() }
    }

    private fun consumer(group: String, listener: BlockListener): ConsumerBatchWorker<*> {
        val kafkaConsumer = RaribleKafkaConsumer(
            clientId = "$clientIdPrefix.block-event-consumer.$service.$group",
            valueDeserializerClass = JsonDeserializer::class.java,
            valueClass = BlockEvent::class.java,
            consumerGroup = group,
            defaultTopic = topic,
            bootstrapServers = properties.brokerReplicaSet,
            offsetResetStrategy = OffsetResetStrategy.EARLIEST,
            properties = mapOf(
                "max.poll.records" to properties.maxPollRecords.toString(),
                "allow.auto.create.topics" to "false"
            )
        )
        return ConsumerBatchWorker(
            consumer = kafkaConsumer,
            properties = daemonProperties,
            // Block consumer should NOT skip events, so there is we're using endless retry
            retryProperties = RetryProperties(attempts = Integer.MAX_VALUE, delay = Duration.ofMillis(1000)),
            eventHandler = BlockEventHandler(listener),
            meterRegistry = meterRegistry,
            workerName = "block-event-consumer-$group"
        )
    }

    private class BlockEventHandler(
        private val blockListener: BlockListener
    ) : ConsumerBatchEventHandler<BlockEvent> {
        override suspend fun handle(event: List<BlockEvent>) {
            blockListener.onBlockEvents(event)
        }
    }

}
