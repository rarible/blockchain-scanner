package com.rarible.blockchain.scanner.block

import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component

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

    fun getAll(): Flow<Block> = mongo.findAll<Block>().asFlow()

}
