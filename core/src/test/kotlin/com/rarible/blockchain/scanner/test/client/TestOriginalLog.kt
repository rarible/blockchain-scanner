package com.rarible.blockchain.scanner.test.client

data class TestOriginalLog(
    val transactionHash: String,
    val blockHash: String?,
    val testExtra: String,
    val logIndex: Int,
    val topic: String
)