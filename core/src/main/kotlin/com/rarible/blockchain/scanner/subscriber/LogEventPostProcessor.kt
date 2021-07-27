package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import reactor.core.publisher.Mono

interface LogEventPostProcessor<L : Log> {

    fun postProcessLogs(logs: List<L>): Mono<Void?>

}