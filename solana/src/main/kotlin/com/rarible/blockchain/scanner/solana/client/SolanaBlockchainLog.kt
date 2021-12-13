package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog

class SolanaBlockchainLog(
    override val hash: String,
    override val blockHash: String?,
    val event: SolanaBlockEvent
) : BlockchainLog