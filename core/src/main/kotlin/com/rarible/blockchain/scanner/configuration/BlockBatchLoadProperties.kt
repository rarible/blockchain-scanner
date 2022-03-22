package com.rarible.blockchain.scanner.configuration

data class BlockBatchLoadProperties(
    val confirmationBlockDistance: Int = 32,
    val batchSize: Int = 1000,
    val batchBufferSize: Int = 25
)
