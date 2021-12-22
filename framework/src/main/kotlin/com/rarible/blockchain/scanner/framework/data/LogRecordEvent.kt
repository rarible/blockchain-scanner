package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * Serialized log record.
 */
data class LogRecordEvent<T : LogRecord<*, *>>(
    val record: T,
    val source: Source,
    val reverted: Boolean
)
