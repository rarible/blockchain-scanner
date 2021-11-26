package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.migration.ChangeLog00001
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
class EthereumLogRepository(
    private val mongo: ReactiveMongoOperations
) {

    private val logger = LoggerFactory.getLogger(EthereumLogRepository::class.java)

    suspend fun findLogEvent(collection: String, id: ObjectId): EthereumLogRecord<*>? {
        return mongo.findById(id, EthereumLogRecord::class.java, collection).awaitFirstOrNull()
    }

    suspend fun delete(collection: String, record: EthereumLogRecord<*>): EthereumLogRecord<*> {
        mongo.remove(record, collection).awaitFirst()
        return record
    }

    suspend fun findVisibleByKey(
        collection: String,
        transactionHash: String,
        topic: Word,
        index: Int,
        minorLogIndex: Int
    ): EthereumLogRecord<*>? {
        val criteria = Criteria.where("log.transactionHash").isEqualTo(transactionHash)
            .and("log.topic").isEqualTo(topic)
            .and("log.index").isEqualTo(index)
            .and("log.minorLogIndex").isEqualTo(minorLogIndex)
            .and("log.visible").isEqualTo(true)
        return mongo.findOne(
            Query.query(criteria).withHint(ChangeLog00001.VISIBLE_INDEX_NAME),
            EthereumLogRecord::class.java,
            collection
        ).awaitFirstOrNull()
    }

    suspend fun findByKey(
        collection: String,
        transactionHash: String,
        blockHash: Word,
        logIndex: Int,
        minorLogIndex: Int
    ): EthereumLogRecord<*>? {
        val criteria = Criteria.where("log.transactionHash").`is`(transactionHash)
            .and("log.blockHash").`is`(blockHash)
            .and("log.logIndex").`is`(logIndex)
            .and("log.minorLogIndex").`is`(minorLogIndex)
        return mongo.findOne(
            Query(criteria),
            EthereumLogRecord::class.java,
            collection
        ).awaitFirstOrNull()
    }

    suspend fun save(collection: String, event: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return mongo.save(event, collection).awaitFirst()
    }

    fun findPendingLogs(collection: String, topic: Word): Flux<EthereumLogRecord<*>> {
        val criteria = Criteria
            .where("log.topic").isEqualTo(topic)
            .and("log.status").`is`(Log.Status.PENDING)

        return mongo.find(
            Query(criteria),
            EthereumLogRecord::class.java,
            collection
        )
    }

    fun findAndRevert(collection: String, blockHash: Word, topic: Word): Flux<EthereumLogRecord<*>> {
        val criteria = Criteria
            .where("log.blockHash").isEqualTo(blockHash)
            .and("log.topic").isEqualTo(topic)

        // TODO we may use update and find queries here instead of updating one-by-one
        return mongo.find(
            Query(criteria),
            EthereumLogRecord::class.java,
            collection
        ).map {
            logger.info("Reverting EthereumLogRecord: [{}]", it)
            val updatedLog = it.log!!.copy(status = Log.Status.REVERTED, visible = false)
            it.withLog(updatedLog)
        }.flatMap {
            mongo.save(it, collection)
        }
    }

    fun findAndDelete(
        collection: String,
        blockHash: Word,
        topic: Word,
        status: Log.Status? = null
    ): Flux<EthereumLogRecord<*>> {
        val criteria = Criteria
            .where("log.blockHash").isEqualTo(blockHash)
            .and("log.topic").isEqualTo(topic)
        status?.let {
            criteria.and("log.status").isEqualTo(status)
        }

        return mongo.find(Query(criteria), EthereumLogRecord::class.java, collection).flatMap {
            logger.info(
                "Deleting EthereumLogRecord: blockHash='{}', status='{}'",
                it.log!!.blockHash, it.log!!.status
            )
            mongo.remove(it, collection).thenReturn(it)
        }
    }
}