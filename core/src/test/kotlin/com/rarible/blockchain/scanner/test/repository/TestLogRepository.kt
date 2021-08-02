package com.rarible.blockchain.scanner.test.repository

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.model.TestLog
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class TestLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    fun delete(collection: String, event: TestLog): Mono<TestLog> {
        return mongo.remove(event, collection).thenReturn(event)
    }

    fun findByKey(
        collection: String,
        transactionHash: String,
        blockHash: String,
        logIndex: Int,
        minorLogIndex: Int
    ): Mono<TestLog> {
        val c = Criteria().andOperator(
            TestLog::transactionHash isEqualTo transactionHash,
            TestLog::blockHash isEqualTo blockHash,
            TestLog::logIndex isEqualTo logIndex,
            TestLog::minorLogIndex isEqualTo minorLogIndex
        )
        return mongo.findOne(Query(c), TestLog::class.java, collection)
    }

    fun save(collection: String, event: TestLog): Mono<TestLog> {
        return mongo.save(event, collection)
    }

    suspend fun saveAll(collection: String, vararg events: TestLog) {
        events.forEach {
            mongo.save(it, collection).awaitFirstOrNull()
        }
    }

    fun findPendingLogs(collection: String): Flux<TestLog> {
        return mongo.find(
            Query(Criteria.where("status").`is`(Log.Status.PENDING)),
            TestLog::class.java,
            collection
        )
    }

    fun findLogEvent(collection: String, id: ObjectId): Mono<TestLog> {
        return mongo.findById(id, TestLog::class.java, collection)
    }

    fun findAndRevert(collection: String, blockHash: String, topic: String): Flux<TestLog> {
        val query = Query().apply {
            addCriteria(Criteria.where(TestLog::blockHash.name).isEqualTo(blockHash))
            addCriteria(Criteria.where(TestLog::topic.name).isEqualTo(topic))
        }
        return mongo.find(query, TestLog::class.java, collection)
            .map { it.copy(status = Log.Status.REVERTED, visible = false) }
            .flatMap { mongo.save(it, collection) }
    }

    fun findAndDelete(
        collection: String,
        blockHash: String,
        topic: String,
        status: Log.Status? = null
    ): Flux<TestLog> {
        val query = Query().apply {
            addCriteria(Criteria.where(TestLog::blockHash.name).isEqualTo(blockHash))
            addCriteria(Criteria.where(TestLog::topic.name).isEqualTo(topic))
            status?.let { addCriteria(Criteria.where(TestLog::status.name).isEqualTo(it)) }
        }

        return mongo.find(query, TestLog::class.java, collection)
            .flatMap {
                delete(collection, it).thenReturn(it)
            }
    }

}
