package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.Record
import com.rarible.core.common.EventTimeMarks

interface RecordEvent<R : Record> {
    val record: R
    val reverted: Boolean
    val eventTimeMarks: EventTimeMarks
}