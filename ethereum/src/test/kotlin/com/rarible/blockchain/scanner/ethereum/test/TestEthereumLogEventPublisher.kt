package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

class TestEthereumLogEventPublisher : LogEventPublisher {

    val publishedLogRecords: MutableList<LogRecord<*, *>> = CopyOnWriteArrayList()

    val dismissedLogs: MutableMap<String, MutableList<LogRecord<*, *>>> = ConcurrentHashMap()

    override suspend fun publish(logEvent: LogEvent<*, *, *>) {
    }

    override suspend fun publish(groupId: String, source: Source, logRecords: List<LogRecord<*, *>>) {
        publishedLogRecords += logRecords
    }

    override suspend fun publishDismissedLogs(descriptor: Descriptor, source: Source, logs: List<LogRecord<*, *>>) {
        dismissedLogs.computeIfAbsent(descriptor.id) { CopyOnWriteArrayList() } += logs
    }
}
