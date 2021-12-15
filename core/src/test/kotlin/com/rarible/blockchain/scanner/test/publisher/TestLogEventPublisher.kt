package com.rarible.blockchain.scanner.test.publisher

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.publisher.LogEventPublisher

class TestLogEventPublisher : LogEventPublisher {

    override suspend fun publish(logEvent: LogEvent<*, *>) = Unit

    override suspend fun publishDismissedLogs(descriptor: Descriptor, source: Source, logs: List<LogRecord<*, *>>) =
        Unit
}
