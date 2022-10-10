package com.rarible.blockchain.scanner.test.model

data class TestLog(
    val transactionHash: String,
    val topic: String,
    val minorLogIndex: Int,
    val blockHash: String?,
    val blockNumber: Long?,
    val logIndex: Int?,
    val index: Int,
    val extra: String,
    val visible: Boolean,
    val reverted: Boolean
)
