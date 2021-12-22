package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.solana.client.SolanaBlockEvent

data class SolanaLog(
    val transactionHash: String,
    val blockHeight: Long,
    val event: SolanaBlockEvent
) : Log
