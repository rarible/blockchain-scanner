package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventPublisher {

    /**
     * Publish LogEvents merged for the group and sorted with log comparator.
     */
    suspend fun publish(groupId: String, source: Source, logRecords: List<LogRecord<*, *>>)

    /**
     * Publish dismissed LogRecords.
     * TODO: will be unneeded when we move out Ethereum's pending logs to a dedicated lib.
     */
    suspend fun publishDismissedLogs(descriptor: Descriptor, source: Source, logs: List<LogRecord<*, *>>)

}
