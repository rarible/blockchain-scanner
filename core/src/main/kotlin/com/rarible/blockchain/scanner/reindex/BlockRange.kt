package com.rarible.blockchain.scanner.reindex

data class BlockRange(
    val from: Long,
    val to: Long?,
    val batchSize: Int?
)
