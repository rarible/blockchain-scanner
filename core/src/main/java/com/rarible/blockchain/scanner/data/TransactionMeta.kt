package com.rarible.blockchain.scanner.data

data class TransactionMeta(
    val hash: String,
    val blockNumber: Long?,
    val blockHash: String?
)
