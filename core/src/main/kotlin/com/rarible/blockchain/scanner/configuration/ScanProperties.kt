package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val enabled: Boolean = true,
    val skipUntil: Long? = null,
    val runReindexTask: Boolean = true,
    val batchLoad: BlockBatchLoadProperties = BlockBatchLoadProperties()
)
