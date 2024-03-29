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
        coroutineThreadCount: Int = 1
    ) {
        batchedConsumerWorkers += logRecordListeners
            .map { listener ->
                consumerWorkerFactory.create(
                    listener = listener,
                    logRecordType = logRecordType,
                    logRecordMapper = logRecordMapper,
                    logRecordFilters = logRecordFilters,
                    workerCount = workerCount,
                    coroutineThreadCount = coroutineThreadCount
                )
            }
            .onEach { consumer -> consumer.start() }
    }

    override fun close() {
        batchedConsumerWorkers.forEach { consumer -> consumer.close() }
    }
}
