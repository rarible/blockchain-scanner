package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.consumer.LogRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

class KafkaLogRecordEventConsumer<T>(
    private val consumerWorkerFactory: LogRecordConsumerWorkerFactory<T>
) : AutoCloseable {

    private val batchedConsumerWorkers = arrayListOf<ConsumerWorkerHolder<*>>()

    fun start(entityEventListeners: List<LogRecordEventListener>) {
        batchedConsumerWorkers += entityEventListeners
            .map { consumerWorkerFactory.create(it) }
            .onEach { consumer -> consumer.start() }
    }

    override fun close() {
        batchedConsumerWorkers.forEach { consumer -> consumer.close() }
    }
}

