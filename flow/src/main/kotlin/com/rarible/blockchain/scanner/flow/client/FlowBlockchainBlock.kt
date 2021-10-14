package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowBlock
import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import java.time.ZoneOffset

class FlowBlockchainBlock(
    override val meta: BlockMeta
) : BlockchainBlock

fun FlowBlock.blockMeta(): BlockMeta = BlockMeta(
    number = this.height,
    hash = this.id.base16Value,
    parentHash = this.parentId.base16Value,
    timestamp = this.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()
)
