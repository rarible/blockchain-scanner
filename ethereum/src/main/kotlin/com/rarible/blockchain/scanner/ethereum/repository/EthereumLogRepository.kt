package com.rarible.blockchain.scanner.ethereum.repository

import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import scalether.domain.Address

interface EthereumLogRepository : LogStorage {
    suspend fun findLogEvent(id: String): EthereumLogRecord?

    suspend fun delete(record: EthereumLogRecord): EthereumLogRecord

    suspend fun findByKey(
        transactionHash: String,
        topic: Word,
        address: Address,
        index: Int,
        minorLogIndex: Int
    ): EthereumLogRecord?

    suspend fun findLegacyRecord(
        transactionHash: String,
        blockHash: Word,
        index: Int,
        minorLogIndex: Int
    ): EthereumLogRecord?

    suspend fun save(event: EthereumLogRecord): EthereumLogRecord

    suspend fun saveAll(event: Collection<EthereumLogRecord>): List<EthereumLogRecord>

    fun find(blockHash: Word, topic: Word): Flow<EthereumLogRecord>

    suspend fun exists(blockHash: Word, topic: Word): Boolean

    fun findAll(): Flow<EthereumLogRecord>

    fun createIndexes(template: MongockTemplate)

    fun dropIndexes(mongockTemplate: MongockTemplate)
}
