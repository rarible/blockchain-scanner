package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.consumer.TransactionRecordConsumerWorkerFactory
import com.rarible.blockchain.scanner.consumer.TransactionRecordFilter
import com.rarible.blockchain.scanner.consumer.TransactionRecordMapper
import com.rarible.blockchain.scanner.framework.listener.TransactionRecordEventListener
import com.rarible.core.kafka.RaribleKafkaConsumerWorker

class KafkaTransactionRecordEventConsumer(
    private val consumerWorkerFactory: TransactionRecordConsumerWorkerFactory
) : AutoCloseable {

    private val batchedConsumerWorkers = arrayListOf<RaribleKafkaConsumerWorker<*>>()

    fun <T> start(
        transactionRecordListeners: List<TransactionRecordEventListener>,
        transactionRecordType: Class<T>,
        transactionRecordMapper: TransactionRecordMapper<T>,
        transactionRecordFilters: List<TransactionRecordFilter<T>>,
        workerCount: Int,
    ) {
        batchedConsumerWorkers += transactionRecordListeners
            .map { listener ->
                consumerWorkerFactory.create(
                    listener,
                    transactionRecordType,
                    transactionRecordMapper,
                    transactionRecordFilters,
                    workerCount
                )
            }
            .onEach { consumer -> consumer.start() }
    }

    override fun close() {
        batchedConsumerWorkers.forEach { consumer -> consumer.close() }
    }
}
