package com.rarible.blockchain.scanner.consumer.kafka

import com.rarible.blockchain.scanner.consumer.LogRecordFilter
import com.rarible.blockchain.scanner.consumer.LogRecordMapper
import com.rarible.blockchain.scanner.framework.listener.LogRecordEventListener
import com.rarible.core.daemon.sequential.ConsumerBatchEventHandler

internal class KafkaLogRecordEventHandler<T>(
    private val logRecordEventListener: LogRecordEventListener,
    private val mapper: LogRecordMapper<T>,
    private val filters: List<LogRecordFilter<T>>
) : ConsumerBatchEventHandler<T> {

    override suspend fun handle(event: List<T>) {
        logRecordEventListener.onLogRecordEvents(
           event
               .asSequence()
               .filter { filters.all { filter -> filter.filter(it) } }
               .map { mapper.map(it) }
               .toList()
        )
    }
}