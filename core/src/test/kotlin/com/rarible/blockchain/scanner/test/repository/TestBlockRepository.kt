package com.rarible.blockchain.scanner.test.repository

import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.core.publisher.Mono

class TestBlockRepository(
    mongo: ReactiveMongoOperations
) : AbstractMongoRepository<TestBlock, Long>(mongo, TestBlock::class.java) {

    fun getLastBlock(): Mono<TestBlock> {
        return mongo.find(Query().with(Sort.by(Sort.Direction.DESC, "_id")).limit(1), TestBlock::class.java)
            .next()
    }

    suspend fun remove(id: Long) {
        mongo.remove(
            Query(Criteria.where("_id").isEqualTo(id)),
            TestBlock::class.java
        ).awaitFirstOrNull()
    }

}