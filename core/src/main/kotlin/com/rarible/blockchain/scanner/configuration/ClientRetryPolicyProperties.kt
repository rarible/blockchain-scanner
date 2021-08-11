package com.rarible.blockchain.scanner.configuration

import java.time.Duration

class ClientRetryPolicyProperties(
    val delay: Duration = Duration.ofSeconds(2),
    val attempts: Int = 3
)