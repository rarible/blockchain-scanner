package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.Log

class LogEventStatusUpdate<L : Log>(
    val logs: List<LogEvent<L>>,
    val status: Log.Status
)