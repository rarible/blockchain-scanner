package com.rarible.blockchain.scanner.test.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.model.nullVersion

class TestLogRecordEventPublisher : LogRecordEventPublisher {

    val publishedLogRecords: MutableMap<String, MutableList<LogRecordEvent<TestLogRecord>>> = hashMapOf()

    override suspend fun publish(
        groupId: String,
        logRecordEvents: List<LogRecordEvent<*>>
    ) {
        @Suppress("UNCHECKED_CAST")
        publishedLogRecords.getOrPut(groupId) { arrayListOf() } +=
            (logRecordEvents as List<LogRecordEvent<TestLogRecord>>).map { it.copy(record = it.record.nullVersion()) }
    }
}
