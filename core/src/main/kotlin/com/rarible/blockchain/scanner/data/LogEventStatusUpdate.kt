package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.model.LogEvent

class LogEventStatusUpdate<L : LogEvent>(
    val logs: List<RichLogEvent<L>>,
    val status: LogEvent.Status
)