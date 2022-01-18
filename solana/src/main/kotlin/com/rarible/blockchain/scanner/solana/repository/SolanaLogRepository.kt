package com.rarible.blockchain.scanner.solana.repository

import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.stereotype.Component

@Component
class SolanaLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    suspend fun delete(collection: String, record: SolanaLogRecord): SolanaLogRecord =
        mongo.remove(record, collection).thenReturn(record).awaitSingle()

    suspend fun saveAll(collection: String, records: List<SolanaLogRecord>): Flow<SolanaLogRecord> =
        records.asFlow().map { save(collection, it) }

    suspend fun save(collection: String, record: SolanaLogRecord): SolanaLogRecord =
        mongo.save(record, collection).awaitSingle()
}
