package com.rarible.blockchain.scanner.hedera.model

data class HederaLog(
    val blockNumber: Long,
    val blockHash: String,
    val consensusTimestamp: String,
    val transactionHash: String,
    val transactionId: String,
    val minorLogIndex: Int? = null
) : Comparable<HederaLog> {

    override fun compareTo(other: HederaLog): Int = comparator.compare(this, other)

    val stringValue: String
        get() = consensusTimestamp + if (minorLogIndex != null) ":$minorLogIndex" else ""

    override fun toString(): String = stringValue

    private companion object {
        private val comparator =
            compareBy<HederaLog>(
                { it.consensusTimestamp },
                { it.minorLogIndex ?: Int.MIN_VALUE },
            )
    }
}
