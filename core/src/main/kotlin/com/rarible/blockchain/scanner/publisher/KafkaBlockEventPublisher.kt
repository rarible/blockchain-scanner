package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.util.getBlockTopic
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.json.JsonSerializer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.slf4j.LoggerFactory
import java.util.*

class KafkaBlockEventPublisher(
    properties: KafkaProperties,
    environment: String,
    blockchain: String,
    service: String
) : BlockEventPublisher {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaBlockEventPublisher::class.java)
    }

    private val topic = getBlockTopic(environment, service, blockchain)

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.block-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = BlockEvent::class.java,
        defaultTopic = topic,
        bootstrapServers = properties.brokerReplicaSet
    )

    init {
        createTopic(properties.brokerReplicaSet)
    }

    override suspend fun publish(event: BlockEvent) {
        val message = KafkaMessage(
            // Key should be constant in order to send all events to same partition
            // (originally, there should be only one partition)
            id = UUID.randomUUID().toString(),
            key = "0",
            value = event
        )
        kafkaProducer.send(message).ensureSuccess()
    }

    private fun createTopic(brokerReplicaSet: String) {
        val newTopic = NewTopic(topic, 1, 1).run {
            configs(mapOf(TopicConfig.RETENTION_MS_CONFIG to Long.MAX_VALUE.toString()))
        }
        val client = AdminClient.create(
            mutableMapOf<String, Any>("bootstrap.servers" to brokerReplicaSet)
        )
        if (!client.listTopics().names().get().contains(topic)) {
            logger.info("Creating topic '{}'", topic)
            client.createTopics(listOf(newTopic)).all().get()
        }
        client.close()
    }
}
