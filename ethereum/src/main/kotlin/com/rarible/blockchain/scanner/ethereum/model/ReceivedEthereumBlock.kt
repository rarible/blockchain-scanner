package com.rarible.blockchain.scanner.ethereum.model

import scalether.domain.response.Block
import java.time.Instant

data class ReceivedEthereumBlock<T>(
    val block: Block<T>,
    val receivedTime: Instant = Instant.now()
)
