package com.rarible.blockchain.scanner.service.pending

import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.model.RichLogEvent

class LogEventStatusUpdate<L : LogEvent>(
    val logs: List<RichLogEvent<L>>,
    val status: LogEvent.Status
)