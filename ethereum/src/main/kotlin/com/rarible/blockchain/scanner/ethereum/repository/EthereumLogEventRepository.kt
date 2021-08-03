package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.migration.ChangeLog00001
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.core.logging.LoggingUtils
import io.daonomic.rpc.domain.Word
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
    fun delete(collection: String, event: EthereumLogRecord<*>): Mono<EthereumLogRecord<*>> {
        return mongo.remove(event, collection).thenReturn(event)
    }

    fun findVisibleByKey(
        collection: String,
        transactionHash: Word,
        topic: Word,
        index: Int,
        minorLogIndex: Int
    ): Mono<EthereumLogRecord<*>> {
        val criteria = Criteria.where("log.transactionHash").`is`(transactionHash)
            .and("log.topic").`is`(topic)
            .and("log.index").`is`(index)
            .and("log.minorLogIndex").`is`(minorLogIndex)
            .and("log.visible").`is`(true)
        return mongo.findOne(
            Query.query(criteria).withHint(ChangeLog00001.VISIBLE_INDEX_NAME),
            EthereumLogRecord::class.java,
            collection
        )
    }

    fun findByKey(
        collection: String,
        transactionHash: Word,
        blockHash: Word,
        logIndex: Int,
        minorLogIndex: Int
    ): Mono<EthereumLogRecord<*>> {
        val criteria = Criteria.where("log.transactionHash").`is`(transactionHash)
            .and("log.blockHash").`is`(blockHash)
            .and("log.logIndex").`is`(logIndex)
            .and("log.minorLogIndex").`is`(minorLogIndex)
        return mongo.findOne(Query(criteria), EthereumLogRecord::class.java, collection)
    }

    fun save(collection: String, event: EthereumLogRecord<*>): Mono<EthereumLogRecord<*>> {
        return mongo.save(event, collection)
    }

    fun findPendingLogs(collection: String, topic: Word): Flux<EthereumLogRecord<*>> {
        val topicCriteria = Criteria.where("log.topic").isEqualTo(topic)
        val statusCriteria = Criteria.where("log.status").`is`(Log.Status.PENDING)
        val query = Query().apply {
            addCriteria(topicCriteria)
            addCriteria(statusCriteria)
        }
        return mongo.find(
            query,
            EthereumLogRecord::class.java,
            collection
        )
    }

    fun findAndRevert(collection: String, blockHash: Word, topic: Word): Flux<EthereumLogRecord<*>> {
        val blockHashCriteria = Criteria.where("log.blockHash").isEqualTo(blockHash)
        val topicCriteria = Criteria.where("log.topic").isEqualTo(topic)
        val query = Query().apply {
            addCriteria(blockHashCriteria)
            addCriteria(topicCriteria)
        }
        return LoggingUtils.withMarkerFlux { marker ->
            mongo.find(query, EthereumLogRecord::class.java, collection)
                .map {
                    logger.info(marker, "reverting $it")
                    it.withLog(it.log!!.copy(status = Log.Status.REVERTED, visible = false))
                }
                .flatMap { mongo.save(it, collection) }
        }
    }

    fun findAndDelete(
        collection: String,
        blockHash: Word,
        topic: Word,
        status: Log.Status? = null
    ): Flux<EthereumLogRecord<*>> {
        return LoggingUtils.withMarkerFlux { marker ->
            val blockHashCriteria = Criteria.where("log.blockHash").isEqualTo(blockHash)
            val topicCriteria = Criteria.where("log.topic").isEqualTo(topic)
            val statusCriteria = status?.let { Criteria.where("log.status").isEqualTo(it) }

            val query = Query().apply {
                addCriteria(blockHashCriteria)
                addCriteria(topicCriteria)
                statusCriteria?.let { addCriteria(it) }
            }
            mongo
                .find(query, EthereumLogRecord::class.java, collection)
                .flatMap {
                    val log = it.log!!
                    logger.info(marker, "Delete log event: blockHash={}, status={}", log.blockHash, log.status)
                    delete(collection, it).thenReturn(it)
                }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EthereumLogEventRepository::class.java)
    }
}