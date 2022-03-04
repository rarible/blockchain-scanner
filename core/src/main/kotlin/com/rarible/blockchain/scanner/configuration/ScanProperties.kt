package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val enabled: Boolean = true,
    val logProcessingEnabled: Boolean = true,
    val skipUntil: Long? = null,
    val batchLoad: BlockBatchLoadProperties = BlockBatchLoadProperties()
)
