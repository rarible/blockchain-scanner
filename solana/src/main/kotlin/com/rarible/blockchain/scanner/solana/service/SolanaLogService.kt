package com.rarible.blockchain.scanner.solana.service

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import com.rarible.blockchain.scanner.solana.model.SolanaLogStorage
import org.springframework.stereotype.Component

@Component
class SolanaLogService : LogService<SolanaLogRecord, SolanaDescriptor, SolanaLogStorage> {

    override suspend fun delete(descriptor: SolanaDescriptor, record: SolanaLogRecord): SolanaLogRecord =
        descriptor.storage.delete(record)

    override suspend fun save(
        descriptor: SolanaDescriptor,
        records: List<SolanaLogRecord>,
        blockHash: String,
    ): List<SolanaLogRecord> = descriptor.storage.saveAll(records)

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: SolanaDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<SolanaLogRecord> = emptyList()

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: SolanaDescriptor,
        revertedBlockHash: String
    ): List<SolanaLogRecord> = emptyList()
}
