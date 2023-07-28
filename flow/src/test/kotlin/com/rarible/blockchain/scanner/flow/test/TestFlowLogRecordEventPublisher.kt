package com.rarible.blockchain.scanner.flow.test

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import java.util.concurrent.CopyOnWriteArrayList

class TestFlowLogRecordEventPublisher : LogRecordEventPublisher {

    val publishedLogRecords: MutableList<LogRecordEvent> = CopyOnWriteArrayList()

    override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>) {
        publishedLogRecords += logRecordEvents
    }
}
