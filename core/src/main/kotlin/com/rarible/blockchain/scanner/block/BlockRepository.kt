package com.rarible.blockchain.scanner.block

import com.mongodb.client.model.InsertManyOptions
import com.rarible.core.mongo.repository.AbstractMongoRepository
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import org.bson.Document
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.convert.MongoConverter
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findAll
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.inValues
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toMono

@Component
class BlockRepository(
    mongo: ReactiveMongoOperations,
    private val mongoConverter: MongoConverter
) : AbstractMongoRepository<Block, Long>(mongo, Block::class.java) {

    suspend fun getByIds(ids: Collection<Long>): List<Block> {
        val criteria = Criteria("_id").inValues(ids)
        return mongo.find<Block>(Query(criteria)).collectList().awaitFirst()
    }

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

    suspend fun insert(blocks: List<Block>): List<Block> {
        return mongo.insertAll(blocks.toMono(), Block::class.java).collectList().awaitSingle()
    }

    suspend fun insertMissing(blocks: List<Block>): List<Block> {
        mongo.execute(Block::class.java) { collection ->
            val documents = blocks.map { block ->
                val document = Document()
                mongoConverter.write(block, document)
                document
            }
            collection.insertMany(documents, InsertManyOptions().ordered(false))
        }.onErrorComplete().awaitFirstOrNull()
        return blocks
    }

    suspend fun failedCount(): Long {
        val criteria = Criteria(Block::status.name).isEqualTo(BlockStatus.ERROR)
        val query = Query(criteria)
        return mongo.count(query, Block::class.java).awaitSingle()
    }

    fun getAll(): Flow<Block> = mongo.findAll<Block>().asFlow()
}
