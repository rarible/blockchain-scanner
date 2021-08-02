package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.test.model.TestLog

class TestLogEventListener(
    override val topics: Set<String>
) : LogEventListener<TestLog> {

    override suspend fun onLogEvent(log: TestLog) {
    }

}