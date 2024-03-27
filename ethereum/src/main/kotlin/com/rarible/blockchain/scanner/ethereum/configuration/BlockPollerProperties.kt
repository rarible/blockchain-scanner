package com.rarible.blockchain.scanner.ethereum.configuration

import java.time.Duration

data class BlockPollerProperties(
    val period: Duration = Duration.ofSeconds(1)
)
