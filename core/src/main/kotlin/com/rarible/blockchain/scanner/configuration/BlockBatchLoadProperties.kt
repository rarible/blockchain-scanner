package com.rarible.blockchain.scanner.configuration

data class BlockBatchLoadProperties(
    val enabled: Boolean = false,
    val distance: Int = 1000,
    val batchSize: Int = 1000
)
