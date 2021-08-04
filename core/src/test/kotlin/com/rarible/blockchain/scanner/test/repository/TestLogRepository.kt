package com.rarible.blockchain.scanner.test.repository

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class TestLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    fun delete(collection: String, event: TestLogRecord<*>): Mono<TestLogRecord<*>> {
        return mongo.remove(event, collection).thenReturn(event)
    }

    fun findByKey(
        collection: String,
        transactionHash: String,
        blockHash: String,
        logIndex: Int,
        minorLogIndex: Int
    ): Mono<TestLogRecord<*>> {
        val criteria = Criteria.where("log.transactionHash").`is`(transactionHash)
            .and("log.blockHash").`is`(blockHash)
            .and("log.logIndex").`is`(logIndex)
            .and("log.minorLogIndex").`is`(minorLogIndex)
        return mongo.findOne(Query(criteria), TestLogRecord::class.java, collection)
    }

    fun save(collection: String, event: TestLogRecord<*>): Mono<TestLogRecord<*>> {
        return mongo.save(event, collection)
    }

    suspend fun saveAll(collection: String, vararg events: TestLogRecord<*>) {
        events.forEach {
            mongo.save(it, collection).awaitFirstOrNull()
        }
    }

    fun findPendingLogs(collection: String): Flux<TestLogRecord<*>> {
        return mongo.find(
            Query(Criteria.where("log.status").`is`(Log.Status.PENDING)),
            TestLogRecord::class.java,
            collection
        )
    }

    fun findLogEvent(collection: String, id: Long): Mono<TestLogRecord<*>> {
        return mongo.findById(id, TestLogRecord::class.java, collection)
    }

    fun findAndRevert(collection: String, blockHash: String, topic: String): Flux<TestLogRecord<*>> {
        val query = Query().apply {
            addCriteria(Criteria.where("log.blockHash").isEqualTo(blockHash))
            addCriteria(Criteria.where("log.topic").isEqualTo(topic))
        }
        return mongo.find(query, TestLogRecord::class.java, collection)
            .map {
                it.withLog(it.log!!.copy(status = Log.Status.REVERTED, visible = false))
            }
            .flatMap { mongo.save(it, collection) }
    }

    fun findAndDelete(
        collection: String,
        blockHash: String,
        topic: String,
        status: Log.Status? = null
    ): Flux<TestLogRecord<*>> {
        val query = Query().apply {
            addCriteria(Criteria.where("log.blockHash").isEqualTo(blockHash))
            addCriteria(Criteria.where("log.topic").isEqualTo(topic))
            status?.let { addCriteria(Criteria.where("log.status").isEqualTo(it)) }
        }

        return mongo.find(query, TestLogRecord::class.java, collection)
            .flatMap {
                delete(collection, it).thenReturn(it)
            }
    }

    // For tests only
    fun findAll(collection: String): Mono<List<Any>> {
        return mongo.findAll<Any>(collection).collectList()
    }

}
