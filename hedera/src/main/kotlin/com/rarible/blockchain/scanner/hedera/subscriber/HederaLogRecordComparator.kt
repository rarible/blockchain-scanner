package com.rarible.blockchain.scanner.hedera.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.hedera.model.HederaLogRecord

object HederaLogRecordComparator : LogRecordComparator<HederaLogRecord> {
    override fun compare(o1: HederaLogRecord, o2: HederaLogRecord): Int = o1.log.compareTo(o2.log)
}
