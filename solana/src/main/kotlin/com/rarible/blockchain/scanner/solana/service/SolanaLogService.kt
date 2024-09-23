package com.rarible.blockchain.scanner.solana.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.repository.SolanaLogRepository
import org.springframework.stereotype.Component

@Component
class SolanaLogService(
    private val logRepository: SolanaLogRepository
) : LogService<SolanaLogRecord, SolanaDescriptor> {
    override suspend fun delete(descriptor: SolanaDescriptor, record: SolanaLogRecord): SolanaLogRecord =
        logRepository.delete(descriptor.collection, record)

    override suspend fun delete(descriptor: SolanaDescriptor, records: List<SolanaLogRecord>): List<SolanaLogRecord> =
        logRepository.delete(descriptor.collection, records)

    override suspend fun save(
        descriptor: SolanaDescriptor,
        records: List<SolanaLogRecord>,
        blockHash: String,
    ): List<SolanaLogRecord> = logRepository.saveAll(descriptor.collection, records)

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: SolanaDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<SolanaLogRecord> = emptyList()

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: SolanaDescriptor,
        revertedBlockHash: String
    ): List<SolanaLogRecord> = emptyList()

    override suspend fun countByBlockNumber(collection: String, blockNumber: Long): Long {
        return logRepository.countByBlockNumber(collection, blockNumber)
    }
}
