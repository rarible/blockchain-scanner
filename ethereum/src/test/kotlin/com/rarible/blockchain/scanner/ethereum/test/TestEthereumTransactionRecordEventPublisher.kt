package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent
import com.rarible.blockchain.scanner.publisher.TransactionRecordEventPublisher
import java.util.concurrent.CopyOnWriteArrayList

class TestEthereumTransactionRecordEventPublisher : TransactionRecordEventPublisher {

    val publishedTransactionRecords: MutableList<TransactionRecordEvent> = CopyOnWriteArrayList()

    override suspend fun publish(groupId: String, logRecordEvents: List<TransactionRecordEvent>) {
        publishedTransactionRecords += logRecordEvents
    }

}
