package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.RaribleKafkaTopics
import com.rarible.core.kafka.json.JsonSerializer
import kotlinx.coroutines.flow.collect
import java.util.*

class KafkaLogRecordEventPublisher<E>(
    properties: KafkaProperties,
    environment: String,
    blockchain: String,
    service: String,
    private val kafkaLogRecordEventWrapper: KafkaLogRecordEventWrapper<E>,
    private val numberOfPartitionsPerGroup: Int
) : LogRecordEventPublisher {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain)
    private val brokerReplicaSet = properties.brokerReplicaSet

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.log-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = kafkaLogRecordEventWrapper.targetClass,
        defaultTopic = topicPrefix, // ends with .log, not required originally
        bootstrapServers = brokerReplicaSet
    )

    override suspend fun prepareGroup(groupId: String) {
        val topic = getTopic(groupId)
        RaribleKafkaTopics.createTopic(brokerReplicaSet, topic, numberOfPartitionsPerGroup)
    }

    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {
        val topic = getTopic(groupId)
        val messages = logRecordEvents.map {
            KafkaMessage(
                id = UUID.randomUUID().toString(),
                key = it.record.getKey(),
                value = kafkaLogRecordEventWrapper.wrap(it)
            )
        }
        kafkaProducer.send(messages, topic).collect { result -> result.ensureSuccess() }
    }

    private fun getTopic(groupId: String): String = "$topicPrefix.${groupId}"
}
