package com.rarible.blockchain.scanner.flow.model

import com.nftco.flow.sdk.FlowBlock
import java.time.Instant

data class ReceivedFlowBlock(
    val block: FlowBlock,
    val receivedTime: Instant = Instant.now()
)
