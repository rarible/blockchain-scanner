package com.rarible.blockchain.scanner.flow.configuration

import java.time.Duration

data class BlockPollerProperties(
    val period: Duration = Duration.ofSeconds(1)
)