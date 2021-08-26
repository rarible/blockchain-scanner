package com.rarible.blockchain.scanner.flow.mapper

import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import org.springframework.stereotype.Component
import java.time.ZoneOffset

@Component
class FlowLogMapper: LogMapper<FlowBlockchainBlock, FlowBlockchainLog, FlowLog> {
    override fun map(
        block: FlowBlockchainBlock,
        log: FlowBlockchainLog,
        index: Int,
        minorIndex: Int,
        descriptor: Descriptor
    ): FlowLog {

        val event = log.event
        return FlowLog(
            transactionHash = log.hash,
            status = Log.Status.CONFIRMED,
            txIndex = event?.transactionIndex,
            eventIndex = event?.eventIndex,
            type = event?.type,
            payload = event?.payload?.byteStringValue?.toStringUtf8(),
            timestamp = block.block.timestamp.toInstant(ZoneOffset.UTC),
            blockHeight = block.number,
            errorMessage = log.errorMessage
        )
    }
}
