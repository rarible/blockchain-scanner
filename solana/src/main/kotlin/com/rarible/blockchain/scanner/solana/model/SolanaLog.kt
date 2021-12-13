package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.solana.client.SolanaBlockEvent

data class SolanaLog(
    override val transactionHash: String,
    override val status: Log.Status,
    val blockHeight: Long,
    val event: SolanaBlockEvent
) : Log<SolanaLog> {

    override fun withStatus(status: Log.Status): SolanaLog {
        return copy(status = status)
    }
}