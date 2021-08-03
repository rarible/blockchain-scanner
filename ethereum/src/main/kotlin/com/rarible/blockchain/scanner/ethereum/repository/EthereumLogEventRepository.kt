package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.migration.ChangeLog00001
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.core.logging.LoggingUtils
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumLogEventRepository(
    private val mongo: ReactiveMongoOperations
) {
    fun delete(collection: String, event: EthereumLog): Mono<EthereumLog> {
        return mongo.remove(event, collection).thenReturn(event)
    }

    fun findVisibleByKey(
        collection: String,
        transactionHash: Word,
        topic: Word,
        index: Int,
        minorLogIndex: Int
    ): Mono<EthereumLog> {
        val c = Criteria.where("transactionHash").`is`(transactionHash)
            .and("topic").`is`(topic)
            .and("index").`is`(index)
            .and("minorLogIndex").`is`(minorLogIndex)
            .and("visible").`is`(true)
        return mongo.findOne(
            Query.query(c).withHint(ChangeLog00001.VISIBLE_INDEX_NAME),
            EthereumLog::class.java,
            collection
        )
    }

    fun findByKey(
        collection: String,
        transactionHash: Word,
        blockHash: Word,
        logIndex: Int,
        minorLogIndex: Int
    ): Mono<EthereumLog> {
        val c = Criteria().andOperator(
            EthereumLog::transactionHash isEqualTo transactionHash,
            EthereumLog::blockHash isEqualTo blockHash,
            EthereumLog::logIndex isEqualTo logIndex,
            EthereumLog::minorLogIndex isEqualTo minorLogIndex
        )
        return mongo.findOne(Query(c), EthereumLog::class.java, collection)
    }

    fun save(collection: String, event: EthereumLog): Mono<EthereumLog> {
        return mongo.save(event, collection)
    }

    fun findPendingLogs(collection: String, topic: Word): Flux<EthereumLog> {
        val topicCriteria = Criteria.where(EthereumLog::topic.name).isEqualTo(topic)
        val statusCriteria = Criteria.where(EthereumLog::status.name).`is`(Log.Status.PENDING)
        val query = Query().apply {
            addCriteria(topicCriteria)
            addCriteria(statusCriteria)
        }
        return mongo.find(
            query,
            EthereumLog::class.java,
            collection
        )
    }

    fun findLogEvent(collection: String, id: ObjectId): Mono<EthereumLog> {
        return mongo.findById(id, EthereumLog::class.java, collection)
    }

    fun findAndRevert(collection: String, blockHash: Word, topic: Word): Flux<EthereumLog> {
        val blockHashCriteria = Criteria.where(EthereumLog::blockHash.name).isEqualTo(blockHash)
        val topicCriteria = Criteria.where(EthereumLog::topic.name).isEqualTo(topic)
        val query = Query().apply {
            addCriteria(blockHashCriteria)
            addCriteria(topicCriteria)
        }
        return LoggingUtils.withMarkerFlux { marker ->
            mongo.find(query, EthereumLog::class.java, collection)
                .map {
                    logger.info(marker, "reverting $it")
                    it.copy(status = Log.Status.REVERTED, visible = false)
                }
                .flatMap { mongo.save(it, collection) }
        }
    }

    fun findAndDelete(
        collection: String,
        blockHash: Word,
        topic: Word,
        status: Log.Status? = null
    ): Flux<EthereumLog> {
        return LoggingUtils.withMarkerFlux { marker ->
            val blockHashCriteria = Criteria.where(EthereumLog::blockHash.name).isEqualTo(blockHash)
            val topicCriteria = Criteria.where(EthereumLog::topic.name).isEqualTo(topic)
            val statusCriteria = status?.let { Criteria.where(EthereumLog::status.name).isEqualTo(it) }

            val query = Query().apply {
                addCriteria(blockHashCriteria)
                addCriteria(topicCriteria)
                statusCriteria?.let { addCriteria(it) }
            }
            mongo
                .find(query, EthereumLog::class.java, collection)
                .flatMap {
                    logger.info(marker, "Delete log event: blockHash={}, status={}", it.blockHash, it.status)
                    delete(collection, it).thenReturn(it)
                }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EthereumLogEventRepository::class.java)
    }
}