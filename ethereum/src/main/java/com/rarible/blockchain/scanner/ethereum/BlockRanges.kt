package com.rarible.blockchain.scanner.ethereum

import reactor.core.publisher.Flux

object BlockRanges {
    fun getRanges(from: Long, to: Long, step: Long): Flux<LongRange> {
        return Flux.fromIterable((from..to).step(step))
            .map { start ->
                start..minOf(start + step - 1, to)
            }
    }
}