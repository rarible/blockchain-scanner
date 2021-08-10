package com.rarible.blockchain.scanner.configuration

import java.time.Duration

data class PendingBlocksJobProperties(
    val interval: Duration = Duration.ofMinutes(1),
    val initialDelay: Duration = Duration.ofMinutes(1),
    val pendingBlockAgeToCheck: Duration = Duration.ofMinutes(1)
)