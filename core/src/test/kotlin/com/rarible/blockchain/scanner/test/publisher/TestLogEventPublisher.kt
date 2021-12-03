package com.rarible.blockchain.scanner.test.publisher

import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.publisher.LogEventPublisher

class TestLogEventPublisher : LogEventPublisher {

    override suspend fun publish(logs: List<LogRecord<*, *>>) {

    }
}