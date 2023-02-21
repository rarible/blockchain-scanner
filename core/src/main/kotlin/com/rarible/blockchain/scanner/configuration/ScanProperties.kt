package com.rarible.blockchain.scanner.configuration

data class ScanProperties(
    val enabled: Boolean = true,
    val skipUntil: Long? = null,
    val batchLoad: BlockBatchLoadProperties = BlockBatchLoadProperties(),
    val firstAvailableBlock: Long = 0
)