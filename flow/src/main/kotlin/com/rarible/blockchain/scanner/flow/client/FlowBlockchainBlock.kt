package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import java.time.ZoneOffset

data class FlowBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
) : BlockchainBlock {

    constructor(block: FlowBlock) : this(
        number = block.height,
        hash = block.id.base16Value,
        parentHash = block.parentId.base16Value,
        timestamp = block.timestamp.toInstant(ZoneOffset.UTC).epochSecond
    )
}

