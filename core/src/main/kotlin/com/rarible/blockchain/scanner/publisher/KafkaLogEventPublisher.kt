package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.configuration.KafkaProperties
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.util.getLogTopic
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

    private val topic = getLogTopic(environment, service, blockchain)

    private val kafkaProducer = RaribleKafkaProducer(
        clientId = "$environment.$blockchain.log-event-producer.$service",
        valueSerializerClass = JsonSerializer::class.java,
        valueClass = LogRecord::class.java,
        defaultTopic = topic,
        bootstrapServers = properties.brokerReplicaSet
    )

    override suspend fun publish(logs: List<LogRecord<*, *>>) {
        val byTopic = HashMap<String, MutableList<KafkaMessage<LogRecord<*, *>>>>()
        logs.forEach {
            val message = KafkaMessage(
                id = UUID.randomUUID().toString(),
                key = it.getKey(),
                value = it
            )
            byTopic.computeIfAbsent(it.getTopic()) { mutableListOf() }.add(message)
        }
        byTopic.forEach {
            kafkaProducer.send(it.value, it.key).collect()
        }
    }
}