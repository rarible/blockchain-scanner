package com.rarible.blockchain.scanner.configuration

data class BlockConsumeProperties(
    val skipUntil: Long? = null,
    val batchLoad: BlockBatchLoadProperties = BlockBatchLoadProperties()
)
