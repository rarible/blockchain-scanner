package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * Log record with [reverted] flag.
 */
data class LogRecordEvent(
    val record: LogRecord,
    val reverted: Boolean
)
