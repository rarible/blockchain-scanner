package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.util.getLogTopicPrefix
import com.rarible.core.kafka.KafkaMessage
import com.rarible.core.kafka.RaribleKafkaProducer
import com.rarible.core.kafka.json.JsonSerializer
import kotlinx.coroutines.flow.collect
import java.util.*

class KafkaLogEventPublisher(
    properties: KafkaProperties,
    environment: String,
    blockchain: String,
    service: String
) : LogEventPublisher {

    private val topicPrefix = getLogTopicPrefix(environment, service, blockchain)

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.log-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = LogRecordEvent::class.java,
        defaultTopic = topicPrefix, // ends with .log, not required originally
        bootstrapServers = properties.brokerReplicaSet
    )

    override suspend fun publish(logEvent: LogEvent) {
        // TODO add sorting
        val byTopic = HashMap<String, MutableList<KafkaMessage<LogRecordEvent<*>>>>()
        val source = logEvent.blockEvent.source

        // In current implementation we're handling here events from single subscriber group,
        // so there should NOT be different topics, but who knows, maybe one day it will be changed
        logEvent.logEvents.map {
            val descriptor = it.key
            val logs = it.value
            val topic = getTopic(descriptor)
            val messages = logs.map { record -> toKafkaMessage(record, source) }
            byTopic.computeIfAbsent(topic) { mutableListOf() }.addAll(messages)
        }

        byTopic.forEach {
            kafkaProducer.send(it.value, it.key).collect()
        }
    }

    override suspend fun publish(descriptor: Descriptor, source: Source, logs: List<LogRecord<*, *>>) {
        // TODO sorting?
        val topic = getTopic(descriptor)
        val messages = logs.map { record -> toKafkaMessage(record, source) }
        kafkaProducer.send(messages, topic).collect()
    }

    private fun getTopic(descriptor: Descriptor): String {
        return "$topicPrefix.${descriptor.groupId}"
    }

    private fun toKafkaMessage(record: LogRecord<*, *>, source: Source): KafkaMessage<LogRecordEvent<*>> {
        return KafkaMessage(
            id = UUID.randomUUID().toString(),
            key = record.getKey(),
            value = LogRecordEvent(
                source = source,
                record = record
            )
        )
    }
}