package com.rarible.blockchain.scanner.solana.subscriber

import com.rarible.blockchain.scanner.solana.model.SolanaLogRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.stereotype.Component

@Component
@ConditionalOnMissingBean(SolanaLogEventComparator::class)
class SolanaDefaultLogEventComparator : SolanaLogEventComparator {
    override fun compare(o1: SolanaLogRecord<*>, o2: SolanaLogRecord<*>): Int {
        return o1.log.blockHeight.compareTo(o2.log.blockHeight)
    }
}