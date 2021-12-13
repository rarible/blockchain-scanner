package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

data class SolanaLogRecord<LR : SolanaLogRecord<LR>>(
    override val log: SolanaLog
) : LogRecord<SolanaLog, SolanaLogRecord<LR>> {
    override fun withLog(log: SolanaLog): SolanaLogRecord<LR> {
        return copy(log = log)
    }

    override fun getKey(): String {
        return log.eventType
    }
}