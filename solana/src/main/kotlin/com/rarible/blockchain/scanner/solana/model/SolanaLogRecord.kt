package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

data class SolanaLogRecord(
    val log: SolanaLog
) : LogRecord {
    fun withLog(log: SolanaLog): SolanaLogRecord {
        return copy(log = log)
    }

    override fun getKey(): String {
        return log.event.programId
    }
}
