package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import kotlinx.coroutines.flow.toList
import org.springframework.stereotype.Service

@Service
class FlowLogService(
    private val logRepository: FlowLogRepository
) : LogService<FlowLog, FlowLogRecord<*>, FlowDescriptor> {

    override suspend fun delete(descriptor: FlowDescriptor, record: FlowLogRecord<*>): FlowLogRecord<*> =
        logRepository.delete(descriptor.collection, record)

    override suspend fun delete(
        descriptor: FlowDescriptor,
        records: List<FlowLogRecord<*>>
    ): List<FlowLogRecord<*>> = records.map { delete(descriptor, it) }

    override suspend fun save(descriptor: FlowDescriptor, records: List<FlowLogRecord<*>>): List<FlowLogRecord<*>> {
        return logRepository.saveAll(descriptor.collection, records).toList()
    }

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: FlowDescriptor,
        newBlock: FullBlock<*, *>
    ): List<FlowLogRecord<*>> {
        // TODO: there are no pending logs in Flow, so nothing to revert, right?
        return emptyList()
    }

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: FlowDescriptor,
        revertedBlockHash: String
    ): List<FlowLogRecord<*>> {
        // TODO: are we sure that in Flow no logs must be reverted?
        return emptyList()
    }
}
