package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.kafka.RaribleKafkaConsumerWorker

interface LogRecordConsumerWorkerFactory {
    fun <T> create(
        listener: LogRecordEventListener,
        logRecordType: Class<T>,
        logRecordMapper: LogRecordMapper<T>,
        logRecordFilters: List<LogRecordFilter<T>>,
        workerCount: Int,
        coroutineThreadCount: Int = 1
    ): RaribleKafkaConsumerWorker<T>
}
