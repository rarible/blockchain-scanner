package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage

data class LogEvent<R : LogRecord, D : Descriptor<S>, S : LogStorage>(
    val blockEvent: BlockEvent<*>,
    val descriptor: D,
    val logRecordsToInsert: List<R>,
    val logRecordsToUpdate: List<R>
)
