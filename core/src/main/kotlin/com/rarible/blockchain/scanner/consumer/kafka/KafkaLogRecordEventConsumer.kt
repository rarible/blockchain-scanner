package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.consumer.LogRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.LogRecordFilter
import com.rarible.blockchain.scanner.consumer.LogRecordMapper
import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

class KafkaLogRecordEventConsumer(
    private val consumerWorkerFactory: LogRecordConsumerWorkerFactory
) : AutoCloseable {

    private val batchedConsumerWorkers = arrayListOf<ConsumerWorkerHolder<*>>()

    fun <T> start(
        logRecordListeners: List<LogRecordEventListener>,
        logRecordType: Class<T>,
        logRecordMapper: LogRecordMapper<T>,
        logRecordFilters: List<LogRecordFilter<T>>,
    ) {
        batchedConsumerWorkers += logRecordListeners
            .map { listener ->
                consumerWorkerFactory.create(
                    listener,
                    logRecordType,
                    logRecordMapper,
                    logRecordFilters
                )
            }
            .onEach { consumer -> consumer.start() }
    }

    override fun close() {
        batchedConsumerWorkers.forEach { consumer -> consumer.close() }
    }
}

