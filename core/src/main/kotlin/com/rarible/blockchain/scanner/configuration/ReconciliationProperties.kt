package com.rarible.blockchain.scanner.configuration

import java.time.Duration

data class ReconciliationProperties(
    val enabled: Boolean = false,
    val blockHandleParallelism: Int = 50,
    val autoReindex: Boolean = false,
    val reindexBatchSize: Int = 50,
    val reindexParallelism: Int = 10,
    val checkPeriod: Duration = Duration.ofMinutes(1),
    /**
     * Reconciliation job is running behind the process of indexing current blocks up to this amount
     * */
    val blockLag: Int = 32,
)
