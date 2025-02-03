package com.rarible.blockchain.scanner.hedera.model

import com.rarible.blockchain.scanner.hedera.utils.toFixedLengthString

data class HederaLog(
    val blockNumber: Long,
    val blockHash: String,
    val consensusTimestamp: String,
    val transactionHash: String,
    val transactionId: String,
    val transactionIndex: Int,
) : Comparable<HederaLog> {
    override fun compareTo(other: HederaLog): Int = comparator.compare(this, other)

    val stringValue: String
        get() = blockNumber.toFixedLengthString(12) +
                ":$blockHash" +
                ":${transactionIndex.toLong().toFixedLengthString(6)}" +
                ":$transactionHash" +
                ":$transactionId" +
                ":$consensusTimestamp"

    override fun toString(): String = stringValue

    private companion object {
        private val comparator =
            compareBy<HederaLog>(
                { it.blockNumber },
                { it.consensusTimestamp },
                { it.transactionIndex }
            )
    }
}
