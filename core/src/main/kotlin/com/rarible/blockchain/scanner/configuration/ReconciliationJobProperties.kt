package com.rarible.blockchain.scanner.configuration

data class ReconciliationJobProperties(
    val enabled: Boolean = true,
    val batchSize: Long = 100
)