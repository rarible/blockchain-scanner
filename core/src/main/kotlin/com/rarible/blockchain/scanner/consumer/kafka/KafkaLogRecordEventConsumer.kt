package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.consumer.LogRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.LogRecordFilter
import com.rarible.blockchain.scanner.consumer.LogRecordMapper
import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.kafka.RaribleKafkaConsumerWorker

class KafkaLogRecordEventConsumer(
    private val consumerWorkerFactory: LogRecordConsumerWorkerFactory
) : AutoCloseable {

    private val batchedConsumerWorkers = arrayListOf<RaribleKafkaConsumerWorker<*>>()

    fun <T> start(
        logRecordListeners: List<LogRecordEventListener>,
        logRecordType: Class<T>,
        logRecordMapper: LogRecordMapper<T>,
        logRecordFilters: List<LogRecordFilter<T>>,
        workerCount: Int,
    ) {
        batchedConsumerWorkers += logRecordListeners
            .map { listener ->
                consumerWorkerFactory.create(
                    listener,
                    logRecordType,
                    logRecordMapper,
                    logRecordFilters,
                    workerCount
                )
            }
            .onEach { consumer -> consumer.start() }
    }

    override fun close() {
        batchedConsumerWorkers.forEach { consumer -> consumer.close() }
    }
}
