package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor

class LogEventStatusUpdate<L : Log, D : LogEventDescriptor>(
    val logs: List<LogEvent<L, D>>,
    val status: Log.Status
)