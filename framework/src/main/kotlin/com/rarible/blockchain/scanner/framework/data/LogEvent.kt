package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

data class LogEvent<L : Log<L>, R : LogRecord<L, *>>(
    // BlockEvent related to produced LogRecords
    val blockEvent: BlockEvent,
    // Subscriber group of the events
    val groupId: String,
    // LogRecords grouped by Descriptor
    val logRecords: List<R>
)
