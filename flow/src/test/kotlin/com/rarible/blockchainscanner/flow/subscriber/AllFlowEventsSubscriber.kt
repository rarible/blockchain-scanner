package com.rarible.blockchainscanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.client.FlowBlockchainBlock
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainLog
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.framework.model.Log
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import org.springframework.stereotype.Component
import java.time.Instant

@ExperimentalCoroutinesApi
@Component
class AllFlowEventsSubscriber: FlowLogEventSubscriber {

    private val descriptor: FlowDescriptor = FlowDescriptor()

    override fun getDescriptor(): FlowDescriptor {
        return descriptor
    }

    override fun getEventRecords(block: FlowBlockchainBlock, log: FlowBlockchainLog): Flow<FlowLogRecord> = channelFlow {
            send(FlowLogRecord(FlowLog(
                transactionHash = log.hash,
                status = Log.Status.CONFIRMED,
                txIndex = log.event?.transactionIndex,
                eventIndex = log.event?.eventIndex,
                type = log.event?.type,
                payload = log.event?.payload?.stringValue,
                timestamp = Instant.ofEpochSecond(block.timestamp),
                blockHeight = block.number,
                errorMessage = log.errorMessage
            )))
        }
    }
