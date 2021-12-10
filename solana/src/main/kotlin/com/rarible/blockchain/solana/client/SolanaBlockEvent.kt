package com.rarible.blockchain.solana.client

import java.math.BigInteger

sealed class SolanaBlockEvent {
    data class SolanaMintEvent(
        val account: String,
        val amount: BigInteger,
        val mint: String
    ) : SolanaBlockEvent()

    data class SolanaCreateTokenMetadataEvent(
        val name: String,
        val uri: String,
        val symbol: String,
        val creators: List<String>
    ) : SolanaBlockEvent()
}