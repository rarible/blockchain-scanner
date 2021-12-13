package com.rarible.blockchain.scanner.solana.client

sealed class SolanaBlockEvent(
    val programId: String
) {
    data class SolanaMintEvent(
        val parsedParams: Map<String, *>
    ) : SolanaBlockEvent("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

    data class SolanaCreateTokenMetadataEvent(
        val data: String
    ) : SolanaBlockEvent("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
}