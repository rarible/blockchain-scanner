package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.core.common.EventTimeMarks

/**
 * Log record with [reverted] flag.
 */
data class LogRecordEvent(
    override val record: LogRecord,
    override val reverted: Boolean,
    override val eventTimeMarks: EventTimeMarks
): RecordEvent<LogRecord>
