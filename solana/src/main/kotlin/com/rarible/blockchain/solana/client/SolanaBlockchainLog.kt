package com.rarible.blockchain.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog

data class SolanaBlockchainLog(
    override val hash: String,
    override val blockHash: String?,
    override val index: Int
) : BlockchainLog
