package com.rarible.blockchain.scanner.configuration

import java.time.Duration

data class PendingLogsJobProperties(
    val interval: Duration = Duration.ofHours(1),
    val initialDelay: Duration = Duration.ofMinutes(1)
)