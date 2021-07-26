package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.data.RichLogEvent
import com.rarible.blockchain.scanner.framework.model.LogEvent
import reactor.core.publisher.Flux

interface PendingLogService<OB, L : LogEvent> {

    fun markInactive(block: OB, logs: List<RichLogEvent<L>>): Flux<LogEventStatusUpdate<L>>

}