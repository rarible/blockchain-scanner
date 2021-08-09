package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import kotlinx.coroutines.flow.Flow
import org.springframework.stereotype.Service

@Service
class FlowPendingLogService: PendingLogService<FlowBlockchainBlock, FlowLog, FlowLogRecord, FlowDescriptor> {
    override fun getInactive(
        block: FlowBlockchainBlock,
        records: List<LogEvent<FlowLog, FlowLogRecord, FlowDescriptor>>
    ): Flow<LogEventStatusUpdate<FlowLog, FlowLogRecord, FlowDescriptor>> {
        throw UnsupportedOperationException()
    }
}
