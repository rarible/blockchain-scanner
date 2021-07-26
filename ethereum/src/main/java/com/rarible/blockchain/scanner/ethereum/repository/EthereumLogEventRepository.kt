package com.rarible.blockchain.scanner.ethereum.repository

import com.rarible.blockchain.scanner.ethereum.migration.ChangeLog00001
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEvent
import com.rarible.blockchain.scanner.framework.model.LogEvent
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
    fun delete(collection: String, event: EthereumLogEvent): Mono<EthereumLogEvent> {
        return mongo.remove(event, collection).thenReturn(event)
    }

    fun findVisibleByKey(
        collection: String,
        transactionHash: Word,
        topic: Word,
        index: Int,
        minorLogIndex: Int
    ): Mono<EthereumLogEvent> {
        val c = Criteria.where("transactionHash").`is`(transactionHash)
            .and("topic").`is`(topic)
            .and("index").`is`(index)
            .and("minorLogIndex").`is`(minorLogIndex)
            .and("visible").`is`(true)
        return mongo.findOne(
            Query.query(c).withHint(ChangeLog00001.VISIBLE_INDEX_NAME),
            EthereumLogEvent::class.java,
            collection
        )
    }

    fun findByKey(
        collection: String,
        transactionHash: Word,
        blockHash: Word,
        logIndex: Int,
        minorLogIndex: Int
    ): Mono<EthereumLogEvent> {
        val c = Criteria().andOperator(
            EthereumLogEvent::transactionHash isEqualTo transactionHash,
            EthereumLogEvent::blockHash isEqualTo blockHash,
            EthereumLogEvent::logIndex isEqualTo logIndex,
            EthereumLogEvent::minorLogIndex isEqualTo minorLogIndex
        )
        return mongo.findOne(Query(c), EthereumLogEvent::class.java, collection)
    }

    fun save(collection: String, event: EthereumLogEvent): Mono<EthereumLogEvent> {
        return mongo.save(event, collection)
    }

    fun findPendingLogs(collection: String): Flux<EthereumLogEvent> {
        return mongo.find(
            Query(Criteria.where("status").`is`(LogEvent.Status.PENDING)),
            EthereumLogEvent::class.java,
            collection
        )
    }

    fun findLogEvent(collection: String, id: ObjectId): Mono<EthereumLogEvent> {
        return mongo.findById(id, EthereumLogEvent::class.java, collection)
    }

    fun findAndRevert(collection: String, blockHash: Word, topic: Word): Flux<EthereumLogEvent> {
        val blockHashCriteria = Criteria.where(EthereumLogEvent::blockHash.name).isEqualTo(blockHash)
        val topicCriteria = Criteria.where(EthereumLogEvent::topic.name).isEqualTo(topic)
        val query = Query().apply {
            addCriteria(blockHashCriteria)
            addCriteria(topicCriteria)
        }
        return LoggingUtils.withMarkerFlux { marker ->
            mongo.find(query, EthereumLogEvent::class.java, collection)
                .map {
                    logger.info(marker, "reverting $it")
                    it.copy(status = LogEvent.Status.REVERTED, visible = false)
                }
                .flatMap { mongo.save(it, collection) }
        }
    }

    fun findAndDelete(
        collection: String,
        blockHash: Word,
        topic: Word,
        status: LogEvent.Status? = null
    ): Flux<EthereumLogEvent> {
        return LoggingUtils.withMarkerFlux { marker ->
            val blockHashCriteria = Criteria.where(EthereumLogEvent::blockHash.name).isEqualTo(blockHash)
            val topicCriteria = Criteria.where(EthereumLogEvent::topic.name).isEqualTo(topic)
            val statusCriteria = status?.let { Criteria.where(EthereumLogEvent::status.name).isEqualTo(it) }

            val query = Query().apply {
                addCriteria(blockHashCriteria)
                addCriteria(topicCriteria)
                statusCriteria?.let { addCriteria(it) }
            }
            mongo
                .find(query, EthereumLogEvent::class.java, collection)
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