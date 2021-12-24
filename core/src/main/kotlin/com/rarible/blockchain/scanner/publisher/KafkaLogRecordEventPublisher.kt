package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.json.JsonSerializer
import kotlinx.coroutines.flow.collect
import java.util.*

class KafkaLogRecordEventPublisher(
    properties: KafkaProperties,
    environment: String,
    blockchain: String,
    service: String
) : LogRecordEventPublisher {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain)

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.log-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = LogRecordEvent::class.java,
        defaultTopic = topicPrefix, // ends with .log, not required originally
        bootstrapServers = properties.brokerReplicaSet
    )

    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent<*>>) {
        val topic = getTopic(groupId)
        val messages = logRecordEvents.map {
            KafkaMessage(
                id = UUID.randomUUID().toString(),
                key = it.record.getKey(),
                value = it
            )
        }
        kafkaProducer.send(messages, topic).collect { result -> result.ensureSuccess() }
    }

    private fun getTopic(groupId: String): String = "$topicPrefix.${groupId}"
}
