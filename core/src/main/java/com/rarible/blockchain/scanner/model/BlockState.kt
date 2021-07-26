package com.rarible.blockchain.scanner.model

import reactor.core.publisher.Mono

interface BlockState<T> {
    fun getLastKnownBlock(): Mono<Long>
    fun getBlockHash(number: Long): Mono<String>
    fun saveKnownBlock(block: T): Mono<Void>
}