package com.rarible.blockchain.scanner.service

import com.rarible.blockchain.scanner.model.Block
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface BlockService<B : Block> {

    fun findByStatus(status: Block.Status): Flux<B>

    fun getLastBlock(): Mono<Long>

    fun updateBlockStatus(number: Long, status: Block.Status): Mono<Void>

    fun findFirstByIdAsc(): Mono<B>

    fun findFirstByIdDesc(): Mono<B>

}