package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import java.util.concurrent.CopyOnWriteArrayList

class TestEthereumLogRecordEventPublisher : LogRecordEventPublisher {

    val publishedLogRecords: MutableList<LogRecordEvent> = CopyOnWriteArrayList()

    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {
        publishedLogRecords += logRecordEvents
    }
}
