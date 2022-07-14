package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.solana.model.SolanaLog

data class SolanaBlockchainLog(
    val log: SolanaLog,
    val instruction: SolanaInstruction
) : BlockchainLog {
    override fun toString(): String = buildString {
        appendLine("Instruction $log")
        appendLine(instruction.toString().prependIndent("  "))
    }
}
