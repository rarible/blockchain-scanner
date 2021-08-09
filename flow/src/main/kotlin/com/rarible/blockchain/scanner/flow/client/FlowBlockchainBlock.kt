package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import org.bouncycastle.util.encoders.Hex
import org.onflow.sdk.FlowBlock
import java.time.ZoneOffset

class FlowBlockchainBlock(
    val block: FlowBlock
) : BlockchainBlock {

    override val meta: BlockMeta
        get() = BlockMeta(
            number = block.height,
            hash = Hex.toHexString(block.id.bytes),
            parentHash = Hex.toHexString(block.parentId.bytes),
            timestamp = block.timestamp.toEpochSecond(ZoneOffset.UTC)
        )
}
