package com.rarible.blockchain.scanner.ethereum.configuration

import java.time.Duration

data class EthereumPendingLogsJobProperties(
    val interval: Duration = Duration.ofHours(1),
    val initialDelay: Duration = Duration.ofMinutes(1)
)