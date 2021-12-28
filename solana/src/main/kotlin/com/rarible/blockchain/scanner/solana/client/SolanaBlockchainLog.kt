package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog

class SolanaBlockchainLog(
    val hash: String,
    val blockHash: String,
    val event: SolanaBlockEvent
) : BlockchainLog
