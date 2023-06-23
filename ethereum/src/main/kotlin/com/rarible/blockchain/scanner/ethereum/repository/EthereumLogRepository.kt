package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.migration.ChangeLog00001
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.and
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import scalether.domain.Address

@Component
@Suppress("UNCHECKED_CAST")
class EthereumLogRepository(
    private val mongo: ReactiveMongoOperations
) {

    suspend fun findLogEvent(entityType: Class<*>, collection: String, id: String): EthereumLogRecord? {
        return mongo.findById(id, entityType, collection).awaitFirstOrNull() as EthereumLogRecord?
    }

    suspend fun delete(collection: String, record: EthereumLogRecord): EthereumLogRecord {
        mongo.remove(record, collection).awaitFirst()
        return record
    }

    suspend fun findVisibleByKey(
        entityType: Class<*>,
        collection: String,
        transactionHash: String,
        topic: Word,
        address: Address,
        index: Int,
        minorLogIndex: Int
    ): EthereumLogRecord? {
        val criteria = Criteria.where("transactionHash").isEqualTo(transactionHash)
            .and("topic").isEqualTo(topic)
            .and("address").isEqualTo(address)
            .and("index").isEqualTo(index)
            .and("minorLogIndex").isEqualTo(minorLogIndex)
            .and("visible").isEqualTo(true)
        return mongo.findOne(
            Query.query(criteria).withHint(ChangeLog00001.VISIBLE_INDEX_NAME),
            entityType,
            collection
        ).awaitFirstOrNull() as EthereumLogRecord?
    }

    suspend fun findLegacyRecord(
        entityType: Class<*>,
        collection: String,
        transactionHash: String,
        blockHash: Word,
        index: Int,
        minorLogIndex: Int
    ): EthereumLogRecord? {
        val criteria = Criteria.where("transactionHash").isEqualTo(transactionHash)
            .and("blockHash").isEqualTo(blockHash)
            .and("logIndex").isEqualTo(index)
            .and("minorLogIndex").isEqualTo(minorLogIndex)
        return mongo.findOne(
            Query.query(criteria).withHint(ChangeLog00001.UNIQUE_RECORD_INDEX_NAME),
            entityType,
            collection
        ).awaitFirstOrNull() as EthereumLogRecord?
    }

    suspend fun save(collection: String, event: EthereumLogRecord): EthereumLogRecord {
        return mongo.save(event, collection).awaitFirst()
    }

    fun find(
        entityType: Class<*>,
        collection: String,
        blockHash: Word,
        topic: Word
    ): Flow<EthereumLogRecord> {
        val criteria = Criteria
            .where("blockHash").isEqualTo(blockHash)
            .and("topic").isEqualTo(topic)
        return mongo.find(Query(criteria), entityType, collection).asFlow() as Flow<EthereumLogRecord>
    }

    suspend fun countByBlockNumber(
        collection: String,
        blockNumber: Long,
    ): Long {
        val criteria =
            (EthereumLog::blockNumber isEqualTo blockNumber)
                .and(EthereumLog::status).isEqualTo(EthereumBlockStatus.CONFIRMED)

        return mongo.count(Query(criteria), collection).awaitFirst()
    }
}
