package com.rarible.blockchain.scanner.solana.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLog
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.repository.SolanaLogRepository
import kotlinx.coroutines.flow.toList
import org.springframework.stereotype.Component

@Component
class SolanaLogService(
    private val logRepository: SolanaLogRepository
) : LogService<SolanaLogRecord, SolanaDescriptor> {
    override suspend fun delete(descriptor: SolanaDescriptor, record: SolanaLogRecord): SolanaLogRecord =
        logRepository.delete(descriptor.collection, record)

    override suspend fun save(
        descriptor: SolanaDescriptor,
        records: List<SolanaLogRecord>
    ): List<SolanaLogRecord> = logRepository.saveAll(descriptor.collection, records).toList()

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: SolanaDescriptor,
        newBlock: FullBlock<*, *>
    ): List<SolanaLogRecord> = emptyList()

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: SolanaDescriptor,
        revertedBlockHash: String
    ): List<SolanaLogRecord> = emptyList()
}
