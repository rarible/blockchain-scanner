package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.framework.subscriber.LogRecordComparator
import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord

object SolanaLogRecordComparator : LogRecordComparator<SolanaLogRecord> {
    // TODO: this looks to be not enough to compare only the block numbers.
    override fun compare(o1: SolanaLogRecord, o2: SolanaLogRecord): Int =
        o1.log.blockHeight.compareTo(o2.log.blockHeight)
}
