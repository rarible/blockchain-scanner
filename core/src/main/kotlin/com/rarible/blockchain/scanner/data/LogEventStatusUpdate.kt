package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

class LogEventStatusUpdate<L : Log, R : LogRecord<L, *>, D : Descriptor>(
    val logs: List<LogEvent<L, R, D>>,
    val status: Log.Status
)