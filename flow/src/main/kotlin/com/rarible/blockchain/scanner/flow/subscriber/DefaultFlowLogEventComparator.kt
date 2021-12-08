package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.stereotype.Component

@Component
@ConditionalOnMissingBean(FlowLogEventComparator::class)
class DefaultFlowLogEventComparator : FlowLogEventComparator {

    // TODO FLOW - implement in right way
    override fun compare(r1: FlowLogRecord<*>, r2: FlowLogRecord<*>): Int {
        var result = compareBlockNumber(r1, r2)
        if (result == 0) result = compareEventIndex(r1, r2)
        return result
    }

}