package com.rarible.blockchain.scanner.flow.configuration

import java.time.Duration

data class CacheBlockEventsProperties(
    val enabled: Boolean = false,
    val size: Long = 1000,
    val expireAfter: Duration = Duration.ofMinutes(30)
)
