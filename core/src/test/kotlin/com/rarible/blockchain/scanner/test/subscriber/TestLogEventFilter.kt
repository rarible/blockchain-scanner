package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord

class TestLogEventFilter(
    private val transactionsToFilter: Set<String>
) : LogEventFilter<TestLogRecord, TestDescriptor> {

    override suspend fun filter(
        events: List<LogEvent<TestLogRecord, TestDescriptor>>
    ): List<LogEvent<TestLogRecord, TestDescriptor>> {
        return events.map { event ->
            event.copy(
                logRecordsToInsert = event.logRecordsToInsert.filterNot {
                    transactionsToFilter.contains(it.log.transactionHash)
                }
            )
        }
    }
}
