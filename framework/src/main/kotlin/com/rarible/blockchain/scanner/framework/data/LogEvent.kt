package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord

data class LogEvent(
    // BlockEvent related to produced LogRecords
    val blockEvent: BlockEvent,
    // Subscriber group of the events
    val groupId: String,
    // LogRecords grouped by Descriptor
    val logRecords: List<LogRecord<*, *>>
)
