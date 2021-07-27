package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.LogEvent
import reactor.core.publisher.Mono

interface LogEventListener<L : LogEvent> {

    val topics: List<String>

    fun onLogEvent(log: L): Mono<Void>
}