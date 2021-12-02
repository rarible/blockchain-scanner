package com.rarible.blockchain.scanner.flow.repository

import com.rarible.blockchain.scanner.flow.model.FlowBlock
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
interface FlowBlockRepository: ReactiveMongoRepository<FlowBlock, Long> {

    fun findTop1ByOrderByIdDesc(): Mono<FlowBlock>
}
