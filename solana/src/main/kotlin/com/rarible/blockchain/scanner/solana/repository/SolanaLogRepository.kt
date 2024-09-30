package com.rarible.blockchain.scanner.solana.repository

import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.core.mongo.util.div
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import org.jetbrains.annotations.TestOnly
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toMono

@Component
class SolanaLogRepository(
    private val mongo: ReactiveMongoOperations
) {
    private val logger = LoggerFactory.getLogger(SolanaLogRepository::class.java)

    suspend fun delete(collection: String, record: SolanaLogRecord): SolanaLogRecord =
        mongo.remove(record, collection).thenReturn(record).awaitSingle()

    suspend fun delete(collection: String, records: List<SolanaLogRecord>): List<SolanaLogRecord> =
        records.map { delete(collection, it) }

    suspend fun saveAll(collection: String, records: List<SolanaLogRecord>): List<SolanaLogRecord> {
        return try {
            mongo.insertAll(records.toMono(), collection).asFlow().toList()
        } catch (e: DuplicateKeyException) {
            // Workaround log records left from the previous run.
            val logs = records.map { it.log }
            logger.warn("WARN: there are the same log records in the database: $logs")
            try {
                delete(collection, records)
            } catch (e: Exception) {
                logger.error("FAIL: cannot remove records from the database to insert new: $logs", e)
            }
            try {
                val insertedRecords = mongo.insertAll(records.toMono(), collection).asFlow().toList()
                logger.info("SUCCESS: removed and inserted new records to the database: $logs")
                insertedRecords
            } catch (e: Exception) {
                logger.error("FAIL: cannot insert new records to the database: $logs", e)
                throw e
            }
        }
    }

    suspend fun save(collection: String, record: SolanaLogRecord): SolanaLogRecord =
        mongo.save(record, collection).awaitSingle()

    suspend fun countByBlockNumber(collection: String, blockNumber: Long): Long {
        val criteria = SolanaLogRecord::log / SolanaLog::blockNumber isEqualTo blockNumber
        return mongo.count(Query(criteria), collection).awaitSingle()
    }

    @TestOnly
    fun findAll(collection: String): Flow<SolanaLogRecord> =
        mongo.findAll<SolanaLogRecord>(collection).asFlow()
}
