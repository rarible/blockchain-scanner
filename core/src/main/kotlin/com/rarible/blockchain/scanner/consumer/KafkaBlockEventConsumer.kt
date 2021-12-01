package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.util.getBlockTopic
import com.rarible.core.daemon.DaemonWorkerProperties
import com.rarible.core.daemon.sequential.ConsumerEventHandler
import com.rarible.core.daemon.sequential.ConsumerWorker
import com.rarible.core.kafka.RaribleKafkaConsumer
import com.rarible.core.kafka.json.JsonDeserializer
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory

class KafkaBlockEventConsumer(
    private val properties: KafkaProperties,
    private val daemonProperties: DaemonWorkerProperties,
    private val meterRegistry: MeterRegistry,
    host: String,
    environment: String,
    blockchain: String,
    private val service: String
) : BlockEventConsumer {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaBlockEventConsumer::class.java)
    }

    private val topic = getBlockTopic(environment, service, blockchain)
    private val clientIdPrefix = "$environment.$host.${java.util.UUID.randomUUID()}.$blockchain"

    override suspend fun start(handler: Map<String, BlockListener>) {
        createTopic()
        handler.map { consumer(it.key, it.value) }
            .forEach { it.start() }
    }

    private fun consumer(group: String, listener: BlockListener): ConsumerWorker<*> {
        val kafkaConsumer = RaribleKafkaConsumer(
            clientId = "$clientIdPrefix.block-event-consumer.$service.$group",
            valueDeserializerClass = JsonDeserializer::class.java,
            valueClass = BlockEvent::class.java,
            consumerGroup = group,
            defaultTopic = topic,
            bootstrapServers = properties.brokerReplicaSet,
            properties = mapOf(
                "max.poll.records" to properties.maxPollRecords.toString(),
                "allow.auto.create.topics" to "false"
            )
        )
        return ConsumerWorker(
            consumer = kafkaConsumer,
            properties = daemonProperties,
            eventHandler = BlockEventHandler(listener),
            meterRegistry = meterRegistry,
            workerName = "block-event-consumer-$group"
        )
    }

    private fun createTopic() {
        val newTopic = NewTopic(topic, 1, 1)
        val client = AdminClient.create(
            mutableMapOf<String, Any>("bootstrap.servers" to properties.brokerReplicaSet)
        )
        if (!client.listTopics().names().get().contains(topic)) {
            logger.info("Creating topic '{}'", topic)
            client.createTopics(listOf(newTopic)).all().get()
        }
        client.close()
    }

    private class BlockEventHandler(
        private val blockListener: BlockListener
    ) : ConsumerEventHandler<BlockEvent> {
        // TODO there should be batch, need to be implemented in core
        override suspend fun handle(event: BlockEvent) {
            blockListener.onBlockEvents(listOf(event))
        }
    }

}