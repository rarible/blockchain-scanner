package com.rarible.blockchain.scanner.solana.repository

import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component

@Component
class SolanaLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    suspend fun findByLogEventType(entityType: Class<*>, collection: String, eventType: String): SolanaLogRecord<*>? {
        val criteria = Criteria.where("log.eventType").isEqualTo(eventType)
        return mongo.findOne(Query.query(criteria), entityType, collection).awaitSingleOrNull() as SolanaLogRecord<*>?
    }

    suspend fun delete(collection: String, record: SolanaLogRecord<*>): SolanaLogRecord<*> {
        return mongo.remove(record, collection).thenReturn(record).awaitSingle()
    }

    suspend fun saveAll(collection: String, records: List<SolanaLogRecord<*>>): Flow<SolanaLogRecord<*>> =
        records.asFlow().map { save(collection, it) }

    suspend fun save(collection: String, record: SolanaLogRecord<*>): SolanaLogRecord<*> =
        mongo.save(record, collection).awaitSingle()
}