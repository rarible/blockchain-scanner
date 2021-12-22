package com.rarible.blockchain.scanner.test.client

data class TestOriginalLog(
    val transactionHash: String,
    val blockNumber: Long,
    val blockHash: String?,
    val testExtra: String,
    val logIndex: Int,
    val topic: String
)
