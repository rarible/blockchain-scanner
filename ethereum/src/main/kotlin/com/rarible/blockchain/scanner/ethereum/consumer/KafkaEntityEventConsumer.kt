package com.rarible.blockchain.scanner.ethereum.consumer

import com.rarible.blockchain.scanner.ethereum.consumer.factory.ConsumerWorkerFactory
import com.rarible.blockchain.scanner.ethereum.reduce.EntityEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

@Deprecated(message = "Use from framework package", replaceWith = ReplaceWith(
    "com.rarible.blockchain.scanner.consumer.kafka.KafkaLogRecordEventConsumer"
))
class KafkaEntityEventConsumer(
    private val consumerWorkerFactory: ConsumerWorkerFactory
) : AutoCloseable {
    private val batchedConsumerWorkers = arrayListOf<ConsumerWorkerHolder<*>>()

    fun start(entityEventListeners: List<EntityEventListener>) {
        batchedConsumerWorkers += entityEventListeners
            .map { consumerWorkerFactory.create(it) }
            .onEach { consumer -> consumer.start() }
    }

    override fun close() {
        batchedConsumerWorkers.forEach { consumer -> consumer.close() }
    }
}
