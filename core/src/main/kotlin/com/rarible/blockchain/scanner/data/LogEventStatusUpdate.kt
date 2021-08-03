package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log

class LogEventStatusUpdate<L : Log, D : Descriptor>(
    val logs: List<LogEvent<L, D>>,
    val status: Log.Status
)