package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.migration.ChangeLog00001
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.remove
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import scalether.domain.Address

@Component
@Suppress("UNCHECKED_CAST")
class EthereumLogRepository(
    private val mongo: ReactiveMongoOperations
) {

    private val logger = LoggerFactory.getLogger(EthereumLogRepository::class.java)

    suspend fun findLogEvent(entityType: Class<*>, collection: String, id: String): EthereumLogRecord<*>? {
        return mongo.findById(id, entityType, collection).awaitFirstOrNull() as EthereumLogRecord<*>?
    }

    suspend fun delete(collection: String, record: EthereumLogRecord<*>): EthereumLogRecord<*> {
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
    ): EthereumLogRecord<*>? {
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
        ).awaitFirstOrNull() as EthereumLogRecord<*>?
    }

    suspend fun save(collection: String, event: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return mongo.save(event, collection).awaitFirst()
    }

    fun findPendingLogs(entityType: Class<*>, collection: String, topic: Word): Flow<EthereumLogRecord<*>> {
        val criteria = Criteria
            .where("topic").isEqualTo(topic)
            .and("status").`is`(Log.Status.PENDING)

        return mongo.find(
            Query(criteria),
            entityType,
            collection
        ).asFlow() as Flow<EthereumLogRecord<*>>
    }

    fun findAndDelete(
        entityType: Class<*>,
        collection: String,
        blockHash: Word,
        topic: Word,
        status: Log.Status? = null
    ): Flux<EthereumLogRecord<*>> {
        var criteria = Criteria
            .where("blockHash").isEqualTo(blockHash)
            .and("topic").isEqualTo(topic)

        criteria = status?.let {
            criteria.and("status").isEqualTo(status)
        } ?: criteria

        val result = mongo.find(Query(criteria), entityType, collection) as Flux<EthereumLogRecord<*>>

        return result.flatMap {
            logger.info(
                "Deleting EthereumLogRecord: blockHash='{}', status='{}'",
                it.log.blockHash, it.log.status
            )
            mongo.remove(it, collection).thenReturn(it)
        }
    }
}
