package com.rarible.blockchain.scanner.model

data class BlockLogs<OL>(
    val blockHash: String,
    val logs: List<OL>
)