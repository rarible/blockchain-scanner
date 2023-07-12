package com.rarible.blockchain.scanner.configuration

import java.time.Duration

class ClientRetryPolicyProperties(
    val delay: Duration = Duration.ofMillis(125),
    val increment: Duration = Duration.ofMillis(125),
    val attempts: Int = 8,
)

