package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogStorage
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.service.LogService
import kotlinx.coroutines.flow.toList
import org.springframework.stereotype.Service

@Service
class FlowLogService : LogService<FlowLogRecord, FlowDescriptor, FlowLogStorage> {

    override suspend fun save(
        descriptor: FlowDescriptor,
        records: List<FlowLogRecord>,
        blockHash: String,
    ): List<FlowLogRecord> {
        return descriptor.storage.saveAll(records).toList()
    }

    override suspend fun prepareLogsToRevertOnNewBlock(
        descriptor: FlowDescriptor,
        fullBlock: FullBlock<*, *>
    ): List<FlowLogRecord> {
        // TODO: there are no pending logs in Flow, so nothing to revert, right?
        return emptyList()
    }

    override suspend fun prepareLogsToRevertOnRevertedBlock(
        descriptor: FlowDescriptor,
        revertedBlockHash: String
    ): List<FlowLogRecord> {
        // TODO: are we sure that in Flow no logs must be reverted?
        return emptyList()
    }
}
