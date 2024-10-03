package com.rarible.blockchain.scanner.flow.repository

import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import kotlinx.coroutines.flow.Flow

interface FlowLogStorage : LogStorage {
    suspend fun getById(id: String): FlowLogRecord?

    fun findAfterEventIndex(
        transactionHash: String,
        afterEventIndex: Int,
    ): Flow<FlowLogRecord>

    fun findBeforeEventIndex(
        transactionHash: String,
        beforeEventIndex: Int,
    ): Flow<FlowLogRecord>

    suspend fun findByLogEventType(eventType: String): FlowLogRecord?

    suspend fun delete(record: FlowLogRecord): FlowLogRecord

    suspend fun saveAll(records: List<FlowLogRecord>): Flow<FlowLogRecord>

    suspend fun save(record: FlowLogRecord): FlowLogRecord

    fun createIndexes(mongockTemplate: MongockTemplate) {
        // Optional
    }
}
