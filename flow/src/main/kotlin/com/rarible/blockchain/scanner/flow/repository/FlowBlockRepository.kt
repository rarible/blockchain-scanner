package com.rarible.blockchain.scanner.flow.repository

import com.rarible.blockchain.scanner.flow.model.FlowBlock
import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.data.mongodb.repository.Aggregation
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
interface FlowBlockRepository: ReactiveMongoRepository<FlowBlock, Long> {

    fun findAllByStatus(status: Block.Status): Flux<FlowBlock>

    @Aggregation(pipeline = [ "{\$group: { _id: '', total: {\$max: \$id }}}"])
    fun getLastNumber(): Mono<Long?>

    fun findByHash(hash: String): Mono<FlowBlock>
}
