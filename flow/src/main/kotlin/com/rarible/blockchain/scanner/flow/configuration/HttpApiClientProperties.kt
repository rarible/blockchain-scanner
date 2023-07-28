package com.rarible.blockchain.scanner.flow.configuration

import java.net.URI
import java.time.Duration

data class HttpApiClientProperties(
    val attempts: Int = 5,
    val delay: Duration = Duration.ofMillis(500),
    val endpoint: URI = URI.create("https://rest-mainnet.onflow.org/"),
    val proxy: URI? = null,
)
