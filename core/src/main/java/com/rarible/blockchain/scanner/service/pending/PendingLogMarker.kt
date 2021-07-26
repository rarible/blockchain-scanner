package com.rarible.blockchain.scanner.service.pending

import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.model.RichLogEvent
import reactor.core.publisher.Flux

interface PendingLogMarker<OB, L : LogEvent> {

    fun markInactive(block: OB, logs: List<RichLogEvent<L>>): Flux<LogEventStatusUpdate<L>>

}