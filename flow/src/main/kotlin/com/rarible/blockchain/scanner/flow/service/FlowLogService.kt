package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.stereotype.Service

@Service
class FlowLogService(
    private val logRepository: FlowLogRepository
): LogService<FlowLog, FlowLogRecord, FlowDescriptor> {
    override suspend fun delete(descriptor: FlowDescriptor, record: FlowLogRecord): FlowLogRecord =
        logRepository.delete(record).thenReturn(record).awaitSingle()


    override suspend fun save(descriptor: FlowDescriptor, records: List<FlowLogRecord>): List<FlowLogRecord> {
        return logRepository.saveAll(records).collectList().awaitFirst()
    }

    override fun findPendingLogs(descriptor: FlowDescriptor): Flow<FlowLogRecord> {
        return emptyFlow()
    }

    override fun findAndRevert(descriptor: FlowDescriptor, blockHash: String): Flow<FlowLogRecord> {
        return emptyFlow()
    }

    override fun findAndDelete(
        descriptor: FlowDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<FlowLogRecord> {
        return emptyFlow()
    }

    override suspend fun updateStatus(
        descriptor: FlowDescriptor,
        record: FlowLogRecord,
        status: Log.Status
    ): FlowLogRecord =
        logRepository.save(record.withLog(record.log.copy(status = status))).awaitSingle()
}
