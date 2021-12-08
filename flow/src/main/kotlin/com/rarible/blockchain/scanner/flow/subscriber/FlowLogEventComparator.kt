package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator

interface FlowLogEventComparator : LogEventComparator<FlowLog, FlowLogRecord<*>> {

    fun compareEventIndex(r1: FlowLogRecord<*>, r2: FlowLogRecord<*>): Int {
        val logIndex1 = r1.log.eventIndex
        val logIndex2 = r2.log.eventIndex
        return logIndex1.compareTo(logIndex2)
    }

    fun compareBlockNumber(r1: FlowLogRecord<*>, r2: FlowLogRecord<*>): Int {
        val blockNumber1 = r1.log.blockHeight
        val blockNumber2 = r2.log.blockHeight
        return blockNumber1.compareTo(blockNumber2)
    }

}