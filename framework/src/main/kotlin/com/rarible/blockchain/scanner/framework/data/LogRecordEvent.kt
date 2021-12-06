package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord

data class LogRecordEvent<T : LogRecord<*, *>>(
    val source: Source,
    val record: T
)



