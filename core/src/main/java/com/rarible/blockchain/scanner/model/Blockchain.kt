package com.rarible.blockchain.scanner.model

import io.daonomic.rpc.domain.Bytes
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface Blockchain<T> {
    fun getLastKnownBlock(): Mono<Long>
    fun getBlock(hash: Bytes): Mono<T>
    fun getBlock(number: Long): Mono<T>
    fun listenNewBlocks(): Flux<T>
}