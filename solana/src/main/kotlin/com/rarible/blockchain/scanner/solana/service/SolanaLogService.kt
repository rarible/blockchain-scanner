package com.rarible.blockchain.scanner.solana.service

import com.rarible.blockchain.scanner.framework.model.Log
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
) : LogService<SolanaLog, SolanaLogRecord<*>, SolanaDescriptor> {
    override suspend fun delete(descriptor: SolanaDescriptor, record: SolanaLogRecord<*>): SolanaLogRecord<*> {
        return logRepository.delete(descriptor.collection, record)
    }

    override suspend fun save(
        descriptor: SolanaDescriptor,
        records: List<SolanaLogRecord<*>>
    ): List<SolanaLogRecord<*>> {
        return logRepository.saveAll(descriptor.collection, records).toList()
    }

    override suspend fun beforeHandleNewBlock(
        descriptor: SolanaDescriptor,
        blockHash: String
    ): List<SolanaLogRecord<*>> {
        return emptyList()
    }

    override suspend fun findAndDelete(
        descriptor: SolanaDescriptor,
        blockHash: String,
        status: Log.Status?
    ): List<SolanaLogRecord<*>> {
        return emptyList()
    }
}