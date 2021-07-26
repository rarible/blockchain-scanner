package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.model.LogEvent
import reactor.core.publisher.Mono

interface LogEventPostProcessor<L : LogEvent> {
    fun postProcessLogs(logs: List<L>): Mono<Void?>
}