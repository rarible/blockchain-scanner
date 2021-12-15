package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventPublisher {

    /**
     * Publish LogEvents of handled block
     */
    suspend fun publish(logEvent: LogEvent<*, *>)

    /**
     * Publish changed LogRecords of specified descriptor. Should be use only for
     * specific operations not related to block-based events.
     */
    suspend fun publish(descriptor: Descriptor, source: Source, logs: List<LogRecord<*, *>>)

}
