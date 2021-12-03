package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.util.getBlockTopic
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.json.JsonSerializer
import java.util.*

class KafkaBlockEventPublisher(
    properties: KafkaProperties,
    environment: String,
    blockchain: String,
    service: String
) : BlockEventPublisher {

    private val topic = getBlockTopic(environment, service, blockchain)

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.block-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = BlockEvent::class.java,
        defaultTopic = topic,
        bootstrapServers = properties.brokerReplicaSet
    )

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
}