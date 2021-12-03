package com.rarible.blockchain.scanner.configuration

import java.time.Duration

data class ScanRetryPolicyProperties(
    val reconnectDelay: Duration = Duration.ofSeconds(2),
    val reconnectAttempts: Int = Integer.MAX_VALUE
)