package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.daemon.sequential.ConsumerWorkerHolder

interface LogRecordConsumerWorkerFactory<T> {
    fun create(listener: LogRecordEventListener): ConsumerWorkerHolder<T>
}