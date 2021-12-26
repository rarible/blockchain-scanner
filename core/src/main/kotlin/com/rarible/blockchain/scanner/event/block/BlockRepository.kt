package com.rarible.blockchain.scanner.event.block

import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findOne
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class BlockRepository(
    mongo: ReactiveMongoOperations
) : AbstractMongoRepository<Block, Long>(mongo, Block::class.java) {

    suspend fun getLastBlock(): Block? {
        return mongo.find(Query().with(Sort.by(Sort.Direction.DESC, "_id")).limit(1), Block::class.java)
            .next()
            .awaitFirstOrNull()
    }

    suspend fun remove(id: Long) {
        mongo.remove(
            Query(Criteria.where("_id").isEqualTo(id)),
            Block::class.java
        ).awaitFirstOrNull()
    }

    fun findFirstByIdAsc(): Mono<Block> =
        mongo.findOne(Query().with(Sort.by(Sort.Direction.ASC, Block::id.name)))

    fun findFirstByIdDesc(): Mono<Block> =
        mongo.findOne(Query().with(Sort.by(Sort.Direction.DESC, Block::id.name)))

}