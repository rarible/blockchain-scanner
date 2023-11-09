package com.rarible.blockchain.scanner.flow.model

import java.time.Instant

data class ReceivedFlowBlock(
    val block: FlowBlockHeader,
    val receivedTime: Instant = Instant.now()
)
