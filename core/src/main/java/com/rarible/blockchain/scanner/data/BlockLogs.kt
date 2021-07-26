package com.rarible.blockchain.scanner.data

data class BlockLogs<OL>(
    val blockHash: String,
    val logs: List<OL>
)