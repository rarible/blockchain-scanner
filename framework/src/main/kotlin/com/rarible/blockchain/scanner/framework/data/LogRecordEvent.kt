package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.core.common.EventTimeMarks

/**
 * Log record with [reverted] flag.
 */
data class LogRecordEvent(
    val record: LogRecord,
    val reverted: Boolean,
    val eventTimeMarks: EventTimeMarks
)
