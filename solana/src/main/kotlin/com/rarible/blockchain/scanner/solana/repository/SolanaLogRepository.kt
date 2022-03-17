package com.rarible.blockchain.scanner.solana.repository

import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import org.jetbrains.annotations.TestOnly
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toMono

@Component
class SolanaLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    suspend fun delete(collection: String, record: SolanaLogRecord): SolanaLogRecord =
        mongo.remove(record, collection).thenReturn(record).awaitSingle()

    suspend fun delete(collection: String, records: List<SolanaLogRecord>): List<SolanaLogRecord> =
        records.map { delete(collection, it) }

    suspend fun saveAll(collection: String, records: List<SolanaLogRecord>): Flow<SolanaLogRecord> =
        mongo.insertAll(records.toMono(), collection).asFlow()

    suspend fun save(collection: String, record: SolanaLogRecord): SolanaLogRecord =
        mongo.save(record, collection).awaitSingle()

    fun findByBlockHash(collection: String, blockHash: String): Flow<SolanaLogRecord> {
        val criteria = Criteria
            .where("log.blockHash").isEqualTo(blockHash)
        return mongo.find(Query(criteria), SolanaLogRecord::class.java, collection).asFlow()
    }

    @TestOnly
    fun findAll(collection: String): Flow<SolanaLogRecord> =
        mongo.findAll<SolanaLogRecord>(collection).asFlow()
}
