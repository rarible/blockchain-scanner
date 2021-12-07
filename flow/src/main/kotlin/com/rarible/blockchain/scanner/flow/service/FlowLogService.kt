package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
import org.springframework.stereotype.Service

@FlowPreview
@Service
class FlowLogService(
    private val logRepository: FlowLogRepository
) : LogService<FlowLog, FlowLogRecord<*>, FlowDescriptor> {

    override suspend fun delete(descriptor: FlowDescriptor, record: FlowLogRecord<*>): FlowLogRecord<*> =
        logRepository.delete(descriptor.collection, record)

    override suspend fun save(descriptor: FlowDescriptor, records: List<FlowLogRecord<*>>): List<FlowLogRecord<*>> {
        return logRepository.saveAll(descriptor.collection, records).toList()
    }

    override suspend fun findAndDelete(
        descriptor: FlowDescriptor,
        blockHash: String,
        status: Log.Status?
    ): List<FlowLogRecord<*>> {
        return emptyList()
    }

    override suspend fun beforeHandleNewBlock(descriptor: FlowDescriptor, blockHash: String): List<FlowLogRecord<*>> {
        return emptyList()
    }
}
