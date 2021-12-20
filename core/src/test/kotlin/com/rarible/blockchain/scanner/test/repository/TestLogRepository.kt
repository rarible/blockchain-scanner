package com.rarible.blockchain.scanner.test.repository

import com.rarible.blockchain.scanner.test.model.TestLogRecord
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Suppress("UNCHECKED_CAST")
class TestLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    fun delete(collection: String, event: TestLogRecord<*>): Mono<TestLogRecord<*>> {
        return mongo.remove(event, collection).thenReturn(event)
    }

    suspend fun findByKey(
        entityType: Class<*>,
        collection: String,
        transactionHash: String,
        blockHash: String,
        logIndex: Int,
        minorLogIndex: Int
    ): TestLogRecord<*>? {
        val criteria = Criteria.where("log.transactionHash").`is`(transactionHash)
            .and("log.blockHash").`is`(blockHash)
            .and("log.logIndex").`is`(logIndex)
            .and("log.minorLogIndex").`is`(minorLogIndex)
        return mongo.findOne(Query(criteria), entityType, collection).awaitFirstOrNull() as TestLogRecord<*>?
    }

    suspend fun save(collection: String, event: TestLogRecord<*>): TestLogRecord<*> {
        return mongo.save(event, collection).awaitFirst()
    }

    suspend fun saveAll(collection: String, vararg events: TestLogRecord<*>) {
        events.forEach {
            mongo.save(it, collection).awaitFirstOrNull()
        }
    }

    suspend fun findLogEvent(entityType: Class<*>, collection: String, id: Long): TestLogRecord<*>? {
        return mongo.findById(id, entityType, collection).awaitFirstOrNull() as TestLogRecord<*>?
    }

    fun find(
        entityType: Class<*>,
        collection: String,
        blockHash: String,
        topic: String
    ): Flux<TestLogRecord<*>> {
        val query = Query().apply {
            addCriteria(Criteria.where("log.blockHash").isEqualTo(blockHash))
            addCriteria(Criteria.where("log.topic").isEqualTo(topic))
        }
        return mongo.find(query, entityType, collection) as Flux<TestLogRecord<*>>
    }

}
