package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.framework.listener.TransactionRecordEventListener
import com.rarible.core.kafka.RaribleKafkaConsumerWorker

interface TransactionRecordConsumerWorkerFactory {
    fun <T> create(
        listener: TransactionRecordEventListener,
        transactionRecordType: Class<T>,
        transactionRecordMapper: TransactionRecordMapper<T>,
        logRecordFilters: List<TransactionRecordFilter<T>>,
        workerCount: Int,
    ): RaribleKafkaConsumerWorker<T>
}