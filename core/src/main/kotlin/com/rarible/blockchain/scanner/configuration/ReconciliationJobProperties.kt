package com.rarible.blockchain.scanner.configuration

data class ReconciliationJobProperties(
    val enabled: Boolean = false,
    val batchSize: Long = 100
)
