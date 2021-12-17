package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class SolanaBlockchainBlock(
    val slot: Long,
    val parentSlot: Long,
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long
): BlockchainBlock

