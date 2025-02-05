package com.rarible.blockchain.scanner.hedera.model

data class HederaLog(
    val blockNumber: Long,
    val blockHash: String,
    val consensusTimestamp: String,
    val transactionHash: String,
    val transactionId: String,
) : Comparable<HederaLog> {

    override fun compareTo(other: HederaLog): Int = comparator.compare(this, other)

    val stringValue: String = consensusTimestamp

    override fun toString(): String = stringValue

    private companion object {
        private val comparator = compareBy<HederaLog> { it.consensusTimestamp }
    }
}
