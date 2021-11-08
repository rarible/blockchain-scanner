package com.rarible.blockchain.scanner.flow.mapper

import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class FlowLogMapper: LogMapper<FlowBlockchainBlock, FlowBlockchainLog, FlowLog> {
    override fun map(
        block: FlowBlockchainBlock,
        log: FlowBlockchainLog,
        index: Int,
        minorIndex: Int,
        descriptor: Descriptor
    ): FlowLog {
        return FlowLog(
            status = Log.Status.CONFIRMED,
            transactionHash = log.hash,
            eventIndex = log.event.eventIndex,
            eventType = log.event.type,
            timestamp = Instant.ofEpochMilli(block.timestamp),
            blockHeight = block.number,
            blockHash = block.hash
        )
    }
}
