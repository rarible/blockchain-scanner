package com.rarible.blockchain.scanner.data

data class BlockLogs<BL>(
    val blockHash: String,
    val logs: List<BL>
)