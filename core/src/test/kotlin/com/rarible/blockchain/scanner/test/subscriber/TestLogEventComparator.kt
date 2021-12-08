package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord

interface TestLogEventComparator : LogEventComparator<TestLog, TestLogRecord<*>> {

    fun compareMinorLogIndex(r1: TestLogRecord<*>, r2: TestLogRecord<*>): Int {
        val logIndex1 = r1.log.minorLogIndex
        val logIndex2 = r2.log.minorLogIndex
        return logIndex1.compareTo(logIndex2)
    }

    fun compareLogIndex(r1: TestLogRecord<*>, r2: TestLogRecord<*>): Int {
        val logIndex1 = r1.log.logIndex ?: -1
        val logIndex2 = r2.log.logIndex ?: -1
        return logIndex1.compareTo(logIndex2)
    }

    fun compareBlockNumber(r1: TestLogRecord<*>, r2: TestLogRecord<*>): Int {
        val blockNumber1 = r1.log.blockNumber ?: -1 // Pending logs should be first in result list
        val blockNumber2 = r2.log.blockNumber ?: -1
        return blockNumber1.compareTo(blockNumber2)
    }

}