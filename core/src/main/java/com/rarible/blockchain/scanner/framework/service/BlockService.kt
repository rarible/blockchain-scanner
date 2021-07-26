package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Block
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface BlockService<B : Block> {

    fun findByStatus(status: Block.Status): Flux<B>

    fun getLastBlock(): Mono<Long>

    fun getBlockHash(id: Long): Mono<String>

    fun updateBlockStatus(id: Long, status: Block.Status): Mono<Void>

    fun saveBlock(block: B): Mono<Void>

    fun findFirstByIdAsc(): Mono<B>

    fun findFirstByIdDesc(): Mono<B>

}