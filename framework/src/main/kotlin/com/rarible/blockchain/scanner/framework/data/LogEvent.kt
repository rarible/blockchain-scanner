package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

data class LogEvent<L : Log, R : LogRecord<L>, D: Descriptor>(
    val blockEvent: BlockEvent,
    val descriptor: D,
    val logRecordsToInsert: List<R>,
    val logRecordsToRemove: List<R>
)
