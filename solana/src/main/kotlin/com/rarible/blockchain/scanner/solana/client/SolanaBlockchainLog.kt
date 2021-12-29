package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaLog

class SolanaBlockchainLog(
    val log: SolanaLog,
    val instruction: SolanaInstruction
) : BlockchainLog
