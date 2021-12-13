package com.rarible.blockchain.scanner.solana.client

import java.math.BigInteger

sealed class SolanaBlockEvent(
    val type: String
){
    data class SolanaMintEvent(
        val account: String,
        val amount: BigInteger,
        val mint: String
    ) : SolanaBlockEvent("Mint")

    data class SolanaCreateTokenMetadataEvent(
        val name: String,
        val uri: String,
        val symbol: String,
        val creators: List<String>
    ) : SolanaBlockEvent("CreateTokenMetadata")
}