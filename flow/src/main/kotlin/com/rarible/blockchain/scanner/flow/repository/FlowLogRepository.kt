package com.rarible.blockchain.scanner.flow.repository

import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toMono

@Component
class FlowLogRepository(
    private val mongo: ReactiveMongoTemplate
) {

    suspend fun findByLogEventType(collection: String, eventType: String): FlowLogRecord<*>? {
        val criteria = Criteria.where("log.eventType").isEqualTo(eventType)
        return mongo.findOne(Query.query(criteria), FlowLogRecord::class.java, collection).awaitSingleOrNull()
    }

    suspend fun delete(collection: String, record: FlowLogRecord<*>): FlowLogRecord<*> {
        return mongo.remove(record, collection).thenReturn(record).awaitSingle()
    }

    fun saveAll(collection: String, records: List<FlowLogRecord<*>>): Flow<FlowLogRecord<*>> =
        records.asFlow().map {
            save(collection, it)
        }

    suspend fun save(collection: String, record: FlowLogRecord<*>): FlowLogRecord<*> =
        mongo.save(record, collection).awaitSingle()

    fun insert(collection: String, records: List<FlowLogRecord<*>>): Flow<FlowLogRecord<*>> = flow {
        emitAll(
            mongo.insertAll(records.toMono(), collection).asFlow()
        )
    }
}
