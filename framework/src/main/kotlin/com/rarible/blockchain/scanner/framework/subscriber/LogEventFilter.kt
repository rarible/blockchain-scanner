package com.rarible.blockchain.scanner.framework.subscriber

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * Filter for batch of logs. Invoked for batch contain logs from several blocks and all subscribers
 * in order to allow filter using batch operations.
 */
interface LogEventFilter<R : LogRecord, D : Descriptor> {

    suspend fun filter(events: List<LogEvent<R, D>>): List<LogEvent<R, D>>

}