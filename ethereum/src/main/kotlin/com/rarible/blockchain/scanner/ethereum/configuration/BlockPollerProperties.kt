package com.rarible.blockchain.scanner.ethereum.configuration

import java.time.Duration

data class BlockPollerProperties(
    val period: Duration = Duration.ofSeconds(1),
    val bufferSize: Int = 1,
    val legacy: Boolean = true
)
