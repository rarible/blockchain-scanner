package com.rarible.blockchain.scanner.solana.client

data class SolanaInstruction(
    val programId: String,
    val data: String,
    val accounts: List<String>
)