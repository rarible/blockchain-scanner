package com.rarible.blockchain.scanner.test.client

data class TestOriginalBlock(

    val number: Long,
    val hash: String,
    val parentHash: String?,
    val timestamp: Long,
    val testExtra: String

)