package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator

object EthereumLogRecordComparator : LogRecordComparator<EthereumLogRecord> {
    override fun compare(r1: EthereumLogRecord, r2: EthereumLogRecord): Int {
        return compareBy<EthereumLogRecord> { it.log.blockNumber!! }
            .thenBy { it.log.logIndex!! }
            .thenBy { it.log.minorLogIndex }
            .compare(r1, r2)
    }
}