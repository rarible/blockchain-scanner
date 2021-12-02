package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findOne
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class EthereumBlockRepository(
    mongo: ReactiveMongoOperations
) : AbstractMongoRepository<EthereumBlock, Long>(mongo, EthereumBlock::class.java) {

    private val logger = LoggerFactory.getLogger(EthereumBlockRepository::class.java)

    suspend fun getLastBlock(): EthereumBlock? {
        return mongo.find(Query().with(Sort.by(Sort.Direction.DESC, "_id")).limit(1), EthereumBlock::class.java)
            .next()
            .awaitFirstOrNull()
    }

    suspend fun remove(id: Long) {
        mongo.remove(
            Query(Criteria.where("_id").isEqualTo(id)),
            EthereumBlock::class.java
        ).awaitFirstOrNull()
    }

    fun findFirstByIdAsc(): Mono<EthereumBlock> =
        mongo.findOne(Query().with(Sort.by(Sort.Direction.ASC, EthereumBlock::id.name)))

    fun findFirstByIdDesc(): Mono<EthereumBlock> =
        mongo.findOne(Query().with(Sort.by(Sort.Direction.DESC, EthereumBlock::id.name)))

}