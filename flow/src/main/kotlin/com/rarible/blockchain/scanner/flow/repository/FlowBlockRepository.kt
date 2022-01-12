package com.rarible.blockchain.scanner.flow.repository

import com.rarible.blockchain.scanner.flow.model.FlowBlock
import com.rarible.blockchain.scanner.framework.model.Block
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface FlowBlockRepository: ReactiveMongoRepository<FlowBlock, Long>, FlowBlockRepositoryCustom {

    fun findAllByStatus(status: Block.Status): Flux<FlowBlock>

    fun findTop1ByOrderByIdDesc(): Mono<FlowBlock>
}

interface FlowBlockRepositoryCustom {
    suspend fun updateStatus(id: Long, status: Block.Status)
}

@Component
class FlowBlockRepositoryCustomImpl(
    private val mongo: ReactiveMongoOperations
): FlowBlockRepositoryCustom {
    override suspend fun updateStatus(id: Long, status: Block.Status) {
        val query = Query(Criteria.where(FlowBlock::id.name).isEqualTo(id))
        val update = Update.update(FlowBlock::status.name, status)
        mongo.findAndModify(query, update, FlowBlock::class.java).awaitSingleOrNull()
    }
}
