package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import reactor.core.publisher.Mono

interface LogEventListener<L : Log> {

    val topics: Set<String>

    fun onLogEvent(log: L): Mono<Void>
}