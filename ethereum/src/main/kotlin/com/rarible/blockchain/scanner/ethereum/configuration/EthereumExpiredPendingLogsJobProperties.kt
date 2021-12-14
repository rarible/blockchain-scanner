package com.rarible.blockchain.scanner.ethereum.configuration

import java.time.Duration

data class EthereumExpiredPendingLogsJobProperties(
    val interval: Duration = Duration.ofHours(1),
    val initialDelay: Duration = Duration.ofMinutes(1),
    val maxPendingLogDuration: Long = Duration.ofHours(2).toMillis()
)
