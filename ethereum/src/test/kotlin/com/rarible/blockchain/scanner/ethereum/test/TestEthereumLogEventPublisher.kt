package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import java.util.concurrent.CopyOnWriteArrayList

class TestEthereumLogEventPublisher : LogEventPublisher {

    val publishedLogEvents: MutableList<LogEvent<*, *>> = CopyOnWriteArrayList()

    override suspend fun publish(logEvent: LogEvent<*, *>) {
        publishedLogEvents += logEvent
    }

    override suspend fun publish(descriptor: Descriptor, source: Source, logs: List<LogRecord<*, *>>): Unit =
        throw UnsupportedOperationException("Not used in the test")
}
