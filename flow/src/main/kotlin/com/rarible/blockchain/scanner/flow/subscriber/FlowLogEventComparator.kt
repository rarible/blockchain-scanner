package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator

object FlowLogEventComparator : LogEventComparator<FlowLog, FlowLogRecord<*>> {
    // TODO: is it even correct?
    override fun compare(r1: FlowLogRecord<*>, r2: FlowLogRecord<*>): Int =
        compareBy<FlowLogRecord<*>> { it.log.blockHeight }
            .thenBy { it.log.eventIndex }
            .compare(r1, r2)

}
