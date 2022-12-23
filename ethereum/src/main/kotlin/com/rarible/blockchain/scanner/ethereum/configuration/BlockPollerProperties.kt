package com.rarible.blockchain.scanner.ethereum.configuration

import java.time.Duration

data class BlockPollerProperties(
    val enabled: Boolean = false,
    val period: Duration = Duration.ofSeconds(1)
)