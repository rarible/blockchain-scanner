package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord

object SolanaLogRecordComparator : LogRecordComparator<SolanaLogRecord> {
    override fun compare(o1: SolanaLogRecord, o2: SolanaLogRecord): Int = o1.log.compareTo(o2.log)
}
