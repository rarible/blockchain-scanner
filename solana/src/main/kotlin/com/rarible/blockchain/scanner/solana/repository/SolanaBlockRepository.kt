package com.rarible.blockchain.scanner.solana.repository

import com.rarible.blockchain.scanner.solana.model.SolanaBlock
import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component

@Component
class SolanaBlockRepository(
    mongo: ReactiveMongoOperations
) : AbstractMongoRepository<SolanaBlock, Long>(mongo, SolanaBlock::class.java) {
    suspend fun getLastBlock(): SolanaBlock? {
        return mongo.find(Query().with(Sort.by(Sort.Direction.DESC, "_id")).limit(1), SolanaBlock::class.java)
            .next()
            .awaitFirstOrNull()
    }

    suspend fun remove(id: Long) {
        mongo.remove(
            Query(Criteria.where("_id").isEqualTo(id)),
            SolanaBlock::class.java
        ).awaitFirstOrNull()
    }
}