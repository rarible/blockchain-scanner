package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.test.model.TestLogRecord


class DefaultTestLogRecordComparator : TestLogEventComparator {

    override fun compare(r1: TestLogRecord<*>, r2: TestLogRecord<*>): Int {
        var result = compareBlockNumber(r1, r2)
        if (result == 0) result = compareLogIndex(r1, r2)
        if (result == 0) result = compareMinorLogIndex(r1, r2)
        return result
    }

}