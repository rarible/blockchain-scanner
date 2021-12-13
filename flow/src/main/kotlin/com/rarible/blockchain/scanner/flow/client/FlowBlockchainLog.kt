package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainLog

data class FlowBlockchainLog(
    override val hash: String,
    override val blockHash: String,
    override val index: Int,
    val event: FlowEvent
) : BlockchainLog
