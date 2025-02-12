package com.rarible.blockchain.scanner.hedera.model

import com.rarible.blockchain.scanner.hedera.utils.toFixedLengthString

data class HederaLog(
    val blockNumber: Long,
    val blockHash: String,
    val consensusTimestamp: String,
    val transactionHash: String,
    val transactionId: String,
    val minorLogIndex: Int = 0
) : Comparable<HederaLog> {

    override fun compareTo(other: HederaLog): Int = comparator.compare(this, other)

    val stringValue: String
        get() = "$consensusTimestamp:${minorLogIndex.toLong().toFixedLengthString(6)}"

    override fun toString(): String = stringValue

    private companion object {
        private val comparator =
            compareBy<HederaLog>(
                { it.consensusTimestamp },
                { it.minorLogIndex },
            )
    }
}
