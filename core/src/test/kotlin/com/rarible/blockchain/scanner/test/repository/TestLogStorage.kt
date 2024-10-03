package com.rarible.blockchain.scanner.test.repository

import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.core.publisher.Flux

@Suppress("UNCHECKED_CAST")
class TestLogStorage(
    private val mongo: ReactiveMongoOperations,
    private val collection: String,
    private val entityType: Class<out TestLogRecord>,
) : LogStorage {

    suspend fun delete(record: TestLogRecord): TestLogRecord {
        mongo.remove(record, collection).awaitSingle()
        return record
    }

    suspend fun findByKey(
        transactionHash: String,
        blockHash: String,
        logIndex: Int,
        minorLogIndex: Int
    ): TestLogRecord? {
        val criteria = Criteria.where("log.transactionHash").`is`(transactionHash)
            .and("log.blockHash").`is`(blockHash)
            .and("log.logIndex").`is`(logIndex)
            .and("log.minorLogIndex").`is`(minorLogIndex)
        return mongo.findOne(Query(criteria), entityType, collection).awaitFirstOrNull()
    }

    suspend fun findAllLogs(): List<TestLogRecord> {
        return mongo.findAll(entityType, collection).collectList().awaitSingle()
    }

    suspend fun save(event: TestLogRecord): TestLogRecord {
        return mongo.save(event, collection).awaitFirst()
    }

    suspend fun findLogEvent(id: Long): TestLogRecord? {
        return mongo.findById(id, entityType, collection).awaitFirstOrNull()
    }

    fun find(
        blockHash: String,
        topic: String
    ): Flux<TestLogRecord> {
        val query = Query().apply {
            addCriteria(Criteria.where("log.blockHash").isEqualTo(blockHash))
            addCriteria(Criteria.where("log.topic").isEqualTo(topic))
        }
        return mongo.find(query, entityType, collection) as Flux<TestLogRecord>
    }

    override suspend fun countByBlockNumber(blockNumber: Long): Long {
        val criteria = TestLog::blockNumber isEqualTo blockNumber
        return mongo.count(Query(criteria), collection).awaitSingle()
    }
}
