package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.BlockMeta
import java.time.ZoneOffset

data class FlowBlockchainBlock(
    override val meta: BlockMeta
) : BlockchainBlock

fun FlowBlock.blockMeta(): BlockMeta = BlockMeta(
    number = this.height,
    hash = this.id.base16Value,
    parentHash = this.parentId.base16Value,
    timestamp = this.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()
)
