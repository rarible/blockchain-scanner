package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class SolanaBlockchainBlock(
    val slot: Long,
    val parentSlot: Long,
    val logs: List<SolanaBlockchainLog>,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long
): BlockchainBlock {
    override val number: Long = slot
}

