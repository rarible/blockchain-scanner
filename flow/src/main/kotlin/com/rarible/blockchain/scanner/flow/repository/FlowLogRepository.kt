package com.rarible.blockchain.scanner.flow.repository

import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.core.mongo.util.div
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.ReactiveMongoTemplate
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.and
import org.springframework.data.mongodb.core.query.isEqualTo
import org.springframework.data.mongodb.core.query.toPath

open class FlowLogRepository<R : FlowLogRecord>(
    protected val mongo: ReactiveMongoTemplate,
    protected val entityType: Class<R>,
    protected val collection: String,
) : FlowLogStorage {
    protected val logger: Logger = LoggerFactory.getLogger(javaClass)

    override suspend fun getById(id: String): R? {
        return mongo.findById(id, entityType, collection).awaitSingleOrNull()
    }

    override fun findAfterEventIndex(
        transactionHash: String,
        afterEventIndex: Int,
    ): Flow<R> {
        val criteria = (FlowLogRecord::log / FlowLog::transactionHash isEqualTo transactionHash)
            .and(FlowLogRecord::log / FlowLog::eventIndex).gt(afterEventIndex)

        val query = Query
            .query(criteria)
            .with(Sort.by(Sort.Direction.ASC, "${FlowLogRecord::log.name}.${FlowLog::eventIndex.name}"))

        return mongo.find(query, entityType, collection).asFlow()
    }

    override fun findBeforeEventIndex(
        transactionHash: String,
        beforeEventIndex: Int,
    ): Flow<R> {
        val criteria = (FlowLogRecord::log / FlowLog::transactionHash isEqualTo transactionHash)
            .and(FlowLogRecord::log / FlowLog::eventIndex).lt(beforeEventIndex)

        val query = Query
            .query(criteria)
            .with(Sort.by(Sort.Direction.DESC, "${FlowLogRecord::log.name}.${FlowLog::eventIndex.name}"))

        return mongo.find(query, entityType, collection).asFlow()
    }

    override suspend fun findByLogEventType(eventType: String): R? {
        val criteria = Criteria.where("log.eventType").isEqualTo(eventType)
        return mongo.findOne(Query.query(criteria), entityType, collection).awaitSingleOrNull()
    }

    override suspend fun delete(record: FlowLogRecord): FlowLogRecord {
        return mongo.remove(record, collection).thenReturn(record).awaitSingle()
    }

    override suspend fun saveAll(records: List<FlowLogRecord>): Flow<FlowLogRecord> =
        records.asFlow().map { save(it) }

    override suspend fun save(record: FlowLogRecord): FlowLogRecord =
        mongo.save(record, collection).awaitSingle()

    override suspend fun countByBlockNumber(blockNumber: Long): Long {
        val criteria = LOG_BLOCKHEIGHT isEqualTo blockNumber
        return mongo.count(Query(criteria), collection).awaitSingle()
    }

    override fun createIndexes(mongockTemplate: MongockTemplate) {
        logger.info("Creating indexes for $collection")
        val indexOps = mongockTemplate.indexOps(collection)
        ALL_INDEXES.forEach(indexOps::ensureIndex)
    }

    companion object Indexes {
        val LOG_BLOCKHEIGHT = FlowLogRecord::log / FlowLog::blockHeight
        val LOG_BLOCKHEIGHT_INDEX = Index().on(LOG_BLOCKHEIGHT.toPath(), Sort.Direction.ASC).background()

        val ALL_INDEXES = listOf(
            LOG_BLOCKHEIGHT_INDEX,
        )
    }
}
