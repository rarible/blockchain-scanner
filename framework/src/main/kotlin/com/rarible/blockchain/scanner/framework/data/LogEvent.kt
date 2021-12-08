package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.LogRecord

data class LogEvent(
    // BlockEvent related to produced LogRecords
    val blockEvent: BlockEvent,
    // Subscriber group of the events
    val groupId: String,
    // LogEvents grouped by Descriptor
    val logEvents: List<LogRecord<*, *>>
) {

    val totalLogSize = logEvents.size

    fun allRecords(): List<LogRecord<*, *>> {
        return logEvents
    }

}
