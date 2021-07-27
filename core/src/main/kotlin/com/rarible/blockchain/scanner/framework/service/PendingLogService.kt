package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import reactor.core.publisher.Flux

interface PendingLogService<OB, L : Log> {

    fun markInactive(block: OB, logs: List<LogEvent<L>>): Flux<LogEventStatusUpdate<L>>

}