package com.rarible.blockchain.scanner.ethereum.model

data class BlockRange(
    val from: Long,
    val to: Long?,
    val batchSize: Int?
)