package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord

data class LogEvent(
    // BlockEvent related to produced LogRecords
    val blockEvent: BlockEvent,
    // LogEvents grouped by Descriptor
    val logEvents: Map<Descriptor, List<LogRecord<*, *>>>
) {

    val totalLogSize = logEvents.values.sumOf { it.size }

    fun allRecords(): List<LogRecord<*, *>> {
        return logEvents.values.flatten()
    }

}
