package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.test.model.TestLogRecord

object TestLogRecordComparator : LogRecordComparator<TestLogRecord> {
    override fun compare(r1: TestLogRecord, r2: TestLogRecord): Int {
        return compareBy<TestLogRecord> { it.log.blockNumber }
            .thenBy { it.log.logIndex }
            .thenBy { it.log.minorLogIndex }
            .compare(r1, r2)
    }
}
