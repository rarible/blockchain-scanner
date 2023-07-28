package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.consumer.TransactionRecordFilter
import com.rarible.blockchain.scanner.consumer.TransactionRecordMapper
import com.rarible.blockchain.scanner.framework.listener.TransactionRecordEventListener
import com.rarible.core.daemon.sequential.ConsumerBatchEventHandler

internal class KafkaTransactionRecordEventHandler<T>(
    private val logRecordEventListener: TransactionRecordEventListener,
    private val mapper: TransactionRecordMapper<T>,
    private val filters: List<TransactionRecordFilter<T>>
) : ConsumerBatchEventHandler<T> {

    override suspend fun handle(event: List<T>) {
        logRecordEventListener.onTransactionRecordEvents(
            event
                .asSequence()
                .filter { filters.all { filter -> filter.filter(it) } }
                .map { mapper.map(it) }
                .toList()
        )
    }
}
