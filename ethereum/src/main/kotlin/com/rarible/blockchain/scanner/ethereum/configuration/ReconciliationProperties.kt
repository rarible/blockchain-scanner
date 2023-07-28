package com.rarible.blockchain.scanner.ethereum.configuration

import java.time.Duration

data class ReconciliationProperties(
    val enabled: Boolean = false,
    val autoReindex: Boolean = false,
    val batchSize: Int = 50,
    val checkPeriod: Duration = Duration.ofMinutes(1),
)
