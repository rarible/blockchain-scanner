package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowBlockRepository
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.stereotype.Service

@Service
class FlowLogService(
    private val logRepository: FlowLogRepository,
    private val blockRepository: FlowBlockRepository
): LogService<FlowLog, FlowLogRecord, FlowDescriptor> {
    override suspend fun delete(descriptor: FlowDescriptor, record: FlowLogRecord): FlowLogRecord =
        logRepository.delete(record).thenReturn(record).awaitSingle()


    override suspend fun save(descriptor: FlowDescriptor, record: FlowLogRecord): FlowLogRecord =
        logRepository.save(record).awaitSingle()


    override fun findPendingLogs(descriptor: FlowDescriptor): Flow<FlowLogRecord> {
        throw UnsupportedOperationException("Not supported pending logs")
    }

    override fun findAndRevert(descriptor: FlowDescriptor, blockHash: String): Flow<FlowLogRecord> {
        throw UnsupportedOperationException("Not supported reverts!")
    }

    override fun findAndDelete(
        descriptor: FlowDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<FlowLogRecord> {
        val block = blockRepository.findByHash(hash = blockHash).block()!!
        return logRepository.findByLog_BlockHeight(block.id).flatMap {
            logRepository.delete(it).thenReturn(it)
        }.asFlow()
    }

    override suspend fun updateStatus(
        descriptor: FlowDescriptor,
        record: FlowLogRecord,
        status: Log.Status
    ): FlowLogRecord =
        logRepository.save(record.withLog(record.log.copy(status = status))).awaitSingle()
}
