package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.model.SolanaLogStorage
import com.rarible.core.mongo.util.div
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.kotlin.core.publisher.toMono

class TestSolanaLogStorage(
    private val mongo: ReactiveMongoOperations,
    private val collection: String,
) : SolanaLogStorage {
    private val logger = LoggerFactory.getLogger(javaClass)

    override suspend fun delete(record: SolanaLogRecord): SolanaLogRecord =
        mongo.remove(record, collection).thenReturn(record).awaitSingle()

    private suspend fun delete(records: List<SolanaLogRecord>): List<SolanaLogRecord> =
        records.map { delete(it) }

    override suspend fun saveAll(records: List<SolanaLogRecord>): List<SolanaLogRecord> {
        return try {
            mongo.insertAll(records.toMono(), collection).asFlow().toList()
        } catch (e: DuplicateKeyException) {
            // Workaround log records left from the previous run.
            val logs = records.map { it.log }
            logger.warn("WARN: there are the same log records in the database: $logs")
            try {
                delete(records)
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

    override suspend fun save(record: SolanaLogRecord): SolanaLogRecord =
        mongo.save(record, collection).awaitSingle()

    override suspend fun countByBlockNumber(blockNumber: Long): Long {
        val criteria = SolanaLogRecord::log / SolanaLog::blockNumber isEqualTo blockNumber
        return mongo.count(Query(criteria), collection).awaitSingle()
    }
}
