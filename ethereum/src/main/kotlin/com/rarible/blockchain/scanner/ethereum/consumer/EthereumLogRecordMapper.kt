package com.rarible.blockchain.scanner.ethereum.consumer

import com.rarible.blockchain.scanner.consumer.LogRecordMapper
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecordEvent
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

object EthereumLogRecordMapper : LogRecordMapper<EthereumLogRecordEvent> {
    override fun map(event: EthereumLogRecordEvent): LogRecordEvent {
        return LogRecordEvent(
            record = event.record,
            reverted = event.reverted,
            eventTimeMarks = event.eventTimeMarks
        )
    }
}
