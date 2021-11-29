package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findOne
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumBlockRepository(
    mongo: ReactiveMongoOperations
) : AbstractMongoRepository<EthereumBlock, Long>(mongo, EthereumBlock::class.java) {

    private val logger = LoggerFactory.getLogger(EthereumBlockRepository::class.java)

    fun findByStatus(status: Block.Status): Flux<EthereumBlock> =
        mongo.find(Query(EthereumBlock::status isEqualTo status))

    suspend fun getLastBlock(): EthereumBlock? {
        return mongo.find(Query().with(Sort.by(Sort.Direction.DESC, "_id")).limit(1), EthereumBlock::class.java)
            .next()
            .awaitFirstOrNull()
    }

    suspend fun updateBlockStatus(number: Long, status: Block.Status) {
        logger.info("Updating block #{} with status: {}", number, status)
        mongo.updateFirst(
            Query(EthereumBlock::id isEqualTo number),
            Update().set("status", status),
            EthereumBlock::class.java
        ).awaitFirstOrNull()
    }

    suspend fun remove(id: Long) {
        mongo.remove(Query(EthereumBlock::id isEqualTo id)).awaitFirstOrNull()
    }

    fun findFirstByIdAsc(): Mono<EthereumBlock> =
        mongo.findOne(Query().with(Sort.by(Sort.Direction.ASC, EthereumBlock::id.name)))

    fun findFirstByIdDesc(): Mono<EthereumBlock> =
        mongo.findOne(Query().with(Sort.by(Sort.Direction.DESC, EthereumBlock::id.name)))

}