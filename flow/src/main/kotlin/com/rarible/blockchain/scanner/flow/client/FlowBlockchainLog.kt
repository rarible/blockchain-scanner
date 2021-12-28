package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainLog

data class FlowBlockchainLog(
    val hash: String,
    val blockHash: String,
    val event: FlowEvent
) : BlockchainLog
