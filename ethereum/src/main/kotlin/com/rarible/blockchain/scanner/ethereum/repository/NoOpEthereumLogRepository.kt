package com.rarible.blockchain.scanner.ethereum.repository

import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import scalether.domain.Address

object NoOpEthereumLogRepository : EthereumLogRepository {
    override suspend fun findLogEvent(id: String): EthereumLogRecord? = null

    override suspend fun delete(record: EthereumLogRecord): EthereumLogRecord = record

    override suspend fun findByKey(transactionHash: String, topic: Word, address: Address, index: Int, minorLogIndex: Int): EthereumLogRecord? {
        return null
    }

    override suspend fun findLegacyRecord(transactionHash: String, blockHash: Word, index: Int, minorLogIndex: Int): EthereumLogRecord? {
        return null
    }

    override suspend fun save(event: EthereumLogRecord): EthereumLogRecord {
        throw UnsupportedOperationException("This repository cannot save logs")
    }

    override suspend fun saveAll(event: Collection<EthereumLogRecord>): List<EthereumLogRecord> {
        throw UnsupportedOperationException("This repository cannot save logs")
    }

    override fun find(blockHash: Word, topic: Word): Flow<EthereumLogRecord> {
        return emptyFlow()
    }

    override suspend fun exists(blockHash: Word, topic: Word): Boolean {
        return false
    }

    override fun findAll(): Flow<EthereumLogRecord> {
        return emptyFlow()
    }

    override suspend fun countByBlockNumber(blockNumber: Long): Long {
        return 0L
    }

    override fun createIndexes(template: MongockTemplate) {}

    override fun dropIndexes(mongockTemplate: MongockTemplate) {}
}
