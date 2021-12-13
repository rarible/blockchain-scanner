package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Log

data class SolanaLog(
    override val transactionHash: String,
    override val status: Log.Status,
    val blockHeight: Long,
    val eventType: String
) : Log<SolanaLog> {

    override fun withStatus(status: Log.Status): SolanaLog {
        return copy(status = status)
    }
}