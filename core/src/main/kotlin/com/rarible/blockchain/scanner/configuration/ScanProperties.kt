package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val skipUntil: Long? = null,
    val batchLoad: BlockBatchLoadProperties = BlockBatchLoadProperties()
)
