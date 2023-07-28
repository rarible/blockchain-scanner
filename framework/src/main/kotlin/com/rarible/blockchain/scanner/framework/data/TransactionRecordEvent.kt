package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.core.common.EventTimeMarks

data class TransactionRecordEvent(
    override val record: TransactionRecord,
    override val reverted: Boolean,
    override val eventTimeMarks: EventTimeMarks
) : RecordEvent<TransactionRecord>
