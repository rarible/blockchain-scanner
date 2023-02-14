package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

interface LogRecordConsumerWorkerFactory {
    fun <T> create(
        listener: LogRecordEventListener,
        logRecordType: Class<T>,
        logRecordMapper: LogRecordMapper<T>,
        logRecordFilters: List<LogRecordFilter<T>>,
    ): ConsumerWorkerHolder<T>
}