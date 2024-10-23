package com.rarible.blockchain.scanner.ethereum.repository

import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.index.PartialIndexFilter
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.and
import org.springframework.data.mongodb.core.query.isEqualTo
import reactor.kotlin.core.publisher.toMono
import scalether.domain.Address

open class DefaultEthereumLogRepository(
    private val mongo: ReactiveMongoOperations,
    private val collection: String,
) : EthereumLogRepository {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val entityType = ReversedEthereumLogRecord::class.java

    override suspend fun findLogEvent(id: String): ReversedEthereumLogRecord? {
        return mongo.findById(id, entityType, collection).awaitSingleOrNull()
    }

    override suspend fun delete(record: EthereumLogRecord): EthereumLogRecord {
        mongo.remove(record, collection).awaitSingle()
        return record
    }

    override suspend fun findByKey(
        transactionHash: String,
        topic: Word,
        address: Address,
        index: Int,
        minorLogIndex: Int
    ): ReversedEthereumLogRecord? {
        val criteria = Criteria.where("transactionHash").isEqualTo(transactionHash)
            .and("topic").isEqualTo(topic)
            .and("address").isEqualTo(address)
            .and("index").isEqualTo(index)
            .and("minorLogIndex").isEqualTo(minorLogIndex)
            .and("visible").isEqualTo(true)

        return mongo.findOne(
            Query.query(criteria),
            entityType,
            collection
        ).awaitSingleOrNull()
    }

    override suspend fun findLegacyRecord(
        transactionHash: String,
        blockHash: Word,
        index: Int,
        minorLogIndex: Int
    ): ReversedEthereumLogRecord? {
        val criteria = Criteria.where("transactionHash").isEqualTo(transactionHash)
            .and("blockHash").isEqualTo(blockHash)
            .and("logIndex").isEqualTo(index)
            .and("minorLogIndex").isEqualTo(minorLogIndex)
        return mongo.findOne(
            Query.query(criteria).withHint(UNIQUE_RECORD_INDEX_NAME),
            entityType,
            collection
        ).awaitSingleOrNull()
    }

    override suspend fun save(event: EthereumLogRecord): EthereumLogRecord {
        return mongo.save(event, collection).awaitSingle()
    }

    override suspend fun saveAll(event: Collection<EthereumLogRecord>): List<EthereumLogRecord> {
        return mongo.insertAll(event.toMono(), collection).collectList().awaitSingle()
    }

    override fun find(blockHash: Word, topic: Word): Flow<ReversedEthereumLogRecord> {
        val criteria = Criteria
            .where("blockHash").isEqualTo(blockHash)
            .and("topic").isEqualTo(topic)
        return mongo.find(Query(criteria), entityType, collection).asFlow()
    }

    override suspend fun exists(blockHash: Word, topic: Word): Boolean {
        val criteria = Criteria
            .where("blockHash").isEqualTo(blockHash)
            .and("topic").isEqualTo(topic)
        return mongo.exists(Query(criteria), entityType, collection).awaitSingle()
    }

    override suspend fun countByBlockNumber(blockNumber: Long): Long {
        val criteria =
            (EthereumLog::blockNumber isEqualTo blockNumber)
                .and(EthereumLog::status).isEqualTo(EthereumBlockStatus.CONFIRMED)

        return mongo.count(Query(criteria), collection).awaitSingle()
    }

    override fun findAll(): Flow<ReversedEthereumLogRecord> {
        return mongo.findAll(entityType, collection).asFlow()
    }

    override fun createIndexes(template: MongockTemplate) {
        logger.info("Creating indexes for $collection")
        val indexOps = template.indexOps(collection)
        allIndexes.forEach {
            try {
                indexOps.ensureIndex(it)
            } catch (ex: Throwable) {
                logger.error("Can't creating index $it", ex)
            }
        }
    }

    override fun dropIndexes(mongockTemplate: MongockTemplate) {
        val indexOps = mongockTemplate.indexOps(collection)
        indexOps.indexInfo.forEach {
            if (it.name in INDEXES_TO_DROP) {
                indexOps.dropIndex(it.name)
            }
        }
    }

    companion object {
        private val INDEXES_TO_DROP = listOf(
            "status"
        )

        private const val VISIBLE_INDEX_NAME = "transactionHash_1_topic_1_address_1_index_1_minorLogIndex_1_visible_1"
        private const val UNIQUE_RECORD_INDEX_NAME = "transactionHash_1_blockHash_1_logIndex_1_minorLogIndex_1"

        private val VISIBLE_INDEX = Index()
            .on("transactionHash", Sort.Direction.ASC)
            .on("topic", Sort.Direction.ASC)
            .on("address", Sort.Direction.ASC)
            .on("index", Sort.Direction.ASC)
            .on("minorLogIndex", Sort.Direction.ASC)
            .on("visible", Sort.Direction.ASC)
            .named(VISIBLE_INDEX_NAME)
            .background()
            .unique()
            .partial(PartialIndexFilter.of(Document("visible", true)))

        // This index is not used for queries but only to ensure the consistency of the database.
        private val UNIQUE_RECORD_INDEX = Index()
            .on("transactionHash", Sort.Direction.ASC)
            .on("blockHash", Sort.Direction.ASC)
            .on("logIndex", Sort.Direction.ASC)
            .on("minorLogIndex", Sort.Direction.ASC)
            .named(UNIQUE_RECORD_INDEX_NAME)
            .background()
            .unique()

        private val BLOCKHASH_INDEX = Index()
            .on("blockHash", Sort.Direction.ASC)
            .named("blockHash")
            .background()

        private val BLOCKNUMNBER_INDEX = Index()
            .on(ReversedEthereumLogRecord::blockNumber.name, Sort.Direction.ASC)
            .background()

        private val allIndexes = listOf(
            VISIBLE_INDEX,
            UNIQUE_RECORD_INDEX,
            BLOCKHASH_INDEX,
            BLOCKNUMNBER_INDEX,
        )
    }
}
