package com.rarible.blockchain.scanner.solana.model

data class SolanaLog(
    val blockNumber: Long,
    val transactionHash: String,
    val blockHash: String,
    val transactionIndex: Int,
    val instructionIndex: Int,
    val innerInstructionIndex: Int?
) : Comparable<SolanaLog> {
    override fun compareTo(other: SolanaLog): Int = comparator.compare(this, other)

    companion object {
        private val comparator =
            compareBy<SolanaLog>(
                { it.blockNumber },
                { it.transactionIndex },
                { it.instructionIndex },
                { it.innerInstructionIndex ?: Int.MIN_VALUE }
            )
    }
}
