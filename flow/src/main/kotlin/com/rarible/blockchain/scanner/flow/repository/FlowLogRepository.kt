package com.rarible.blockchain.scanner.flow.repository

import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component

@Component
class FlowLogRepository(
    private val mongo: ReactiveMongoTemplate
) {
    suspend fun getById(id: String, entityType: Class<*>, collection: String): FlowLogRecord? {
        return mongo.findById(id, entityType, collection).awaitSingleOrNull() as FlowLogRecord?
    }

    suspend fun findByLogEventType(entityType: Class<*>, collection: String, eventType: String): FlowLogRecord? {
        val criteria = Criteria.where("log.eventType").isEqualTo(eventType)
        return mongo.findOne(Query.query(criteria), entityType, collection).awaitSingleOrNull() as FlowLogRecord?
    }

    suspend fun delete(collection: String, record: FlowLogRecord): FlowLogRecord {
        return mongo.remove(record, collection).thenReturn(record).awaitSingle()
    }

    suspend fun saveAll(collection: String, records: List<FlowLogRecord>): Flow<FlowLogRecord> =
        records.asFlow().map { save(collection, it) }

    suspend fun save(collection: String, record: FlowLogRecord): FlowLogRecord =
        mongo.save(record, collection).awaitSingle()

}
