package com.rarible.blockchain.scanner.solana.client

data class SolanaBlockEvent(
    val programId: String,
    val data: String,
    val accounts: List<String>
)