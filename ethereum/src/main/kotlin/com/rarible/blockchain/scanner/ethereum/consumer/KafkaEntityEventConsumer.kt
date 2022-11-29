package com.rarible.blockchain.scanner.ethereum.consumer

import com.rarible.blockchain.scanner.ethereum.consumer.factory.ConsumerWorkerFactory
import com.rarible.blockchain.scanner.ethereum.reduce.EntityEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

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

