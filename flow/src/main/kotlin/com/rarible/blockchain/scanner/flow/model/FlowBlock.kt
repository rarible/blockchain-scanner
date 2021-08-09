package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.bouncycastle.util.encoders.Hex
import org.onflow.sdk.FlowBlock
import java.time.ZoneOffset

data class FlowBlock(
    override val id: Long,
    override val hash: String,
    override val parentHash: String,
    override val timestamp: Long,
    override val status: Block.Status
): Block {

    constructor(block: FlowBlock): this(
        id = block.height,
        hash = Hex.toHexString(block.id.bytes),
        parentHash = Hex.toHexString(block.parentId.bytes),
        timestamp = block.timestamp.toEpochSecond(ZoneOffset.UTC),
        status = Block.Status.SUCCESS
    )
}
