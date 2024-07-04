package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.data.RecordEvent
import com.rarible.blockchain.scanner.framework.model.Record
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.RaribleKafkaTopics
import com.rarible.core.kafka.json.JsonSerializer
import java.util.UUID

class KafkaRecordEventPublisher<E, R : Record, RE : RecordEvent<R>>(
    properties: KafkaProperties,
    environment: String,
    blockchain: String,
    service: String,
    type: String,
    private val kafkaRecordEventWrapper: KafkaRecordEventWrapper<E, R, RE>,
    private val numberOfPartitionsPerGroup: Int,
) : RecordEventPublisher<R, RE> {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain, type)
    private val brokerReplicaSet = properties.brokerReplicaSet

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.$type-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = kafkaRecordEventWrapper.targetClass,
        defaultTopic = topicPrefix, // ends with .log, not required originally
        bootstrapServers = brokerReplicaSet,
        compression = properties.compression,
        properties = properties.producerProperties,
    )

    override suspend fun prepareGroup(groupId: String) {
        val topic = getTopic(groupId)
        RaribleKafkaTopics.createTopic(brokerReplicaSet, topic, numberOfPartitionsPerGroup)
    }

    override suspend fun publish(groupId: String, recordEvents: List<RE>) {
        val topic = getTopic(groupId)
        val messages = recordEvents.map {
            KafkaMessage(
                id = UUID.randomUUID().toString(),
                key = it.record.getKey(),
                value = kafkaRecordEventWrapper.wrap(it)
            )
        }
        kafkaProducer.send(messages, topic).collect { result -> result.ensureSuccess() }
    }

    private fun getTopic(groupId: String): String = "$topicPrefix.$groupId"
}
