package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.solana.util.toFixedLengthString

data class SolanaLog(
    val blockNumber: Long,
    val blockHash: String,
    val transactionIndex: Int,
    val transactionHash: String,
    val instructionIndex: Int,
    val innerInstructionIndex: Int?
) : Comparable<SolanaLog> {
    override fun compareTo(other: SolanaLog): Int = comparator.compare(this, other)

    val stringValue: String
        get() = blockNumber.toFixedLengthString(12) +
                ":$blockHash" +
                ":${transactionIndex.toLong().toFixedLengthString(6)}" +
                ":$transactionHash" +
                ":${instructionIndex.toLong().toFixedLengthString(6)}" +
                if (innerInstructionIndex != null)
                    ":${innerInstructionIndex.toLong().toFixedLengthString(6)}"
                else ""

    override fun toString(): String = stringValue

    private companion object {
        private val comparator =
            compareBy<SolanaLog>(
                { it.blockNumber },
                { it.transactionIndex },
                { it.instructionIndex },
                { it.innerInstructionIndex ?: Int.MIN_VALUE }
            )
    }
}
