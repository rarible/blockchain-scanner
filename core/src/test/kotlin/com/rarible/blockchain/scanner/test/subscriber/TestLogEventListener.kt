package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.ProcessedBlockEvent
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord

class TestLogEventListener : LogEventListener<TestLog, TestLogRecord<*>> {

    override suspend fun onBlockLogsProcessed(blockEvent: ProcessedBlockEvent<TestLog, TestLogRecord<*>>) {

    }

    override suspend fun onPendingLogsDropped(logs: List<TestLogRecord<*>>) {

    }
}