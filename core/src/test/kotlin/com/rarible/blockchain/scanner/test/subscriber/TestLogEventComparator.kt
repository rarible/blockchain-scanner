package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogEventComparator
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord

object TestLogEventComparator : LogEventComparator<TestLog, TestLogRecord<*>> {
    override fun compare(r1: TestLogRecord<*>, r2: TestLogRecord<*>): Int {
        return compareBy<TestLogRecord<*>> { it.log.blockNumber }
            .thenBy { it.log.logIndex }
            .thenBy { it.log.minorLogIndex }
            .compare(r1, r2)
    }

}
