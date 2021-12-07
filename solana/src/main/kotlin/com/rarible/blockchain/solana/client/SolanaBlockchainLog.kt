package com.rarible.blockchain.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog

class SolanaBlockchainLog(
    override val hash: String,
    override val blockHash: String?
) : BlockchainLog