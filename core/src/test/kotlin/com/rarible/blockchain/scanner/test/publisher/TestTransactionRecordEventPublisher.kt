package com.rarible.blockchain.scanner.test.publisher

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

class TestTransactionRecordEventPublisher : TransactionRecordEventPublisher {

    val publishedTransactionRecords: MutableMap<String, MutableList<TransactionRecordEvent>> = ConcurrentHashMap()

    override suspend fun publish(
        groupId: String,
        transactionRecordEvents: List<TransactionRecordEvent>
    ) {
        @Suppress("UNCHECKED_CAST")
        publishedTransactionRecords.getOrPut(groupId) { CopyOnWriteArrayList() } +=
            (transactionRecordEvents).map { it }
    }
}
