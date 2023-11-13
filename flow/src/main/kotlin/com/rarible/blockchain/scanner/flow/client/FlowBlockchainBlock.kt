package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.FlowBlock
import com.rarible.blockchain.scanner.flow.model.ReceivedFlowBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import java.time.Instant
import java.time.ZoneOffset

data class FlowBlockchainBlock(
    override val number: Long,
    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    override val receivedTime: Instant = Instant.now(),
) : BlockchainBlock {

    constructor(block: FlowBlock) : this(ReceivedFlowBlock(block))

    constructor(block: ReceivedFlowBlock) : this(
        number = block.block.height,
        hash = block.block.id.base16Value,
        parentHash = block.block.parentId.base16Value,
        receivedTime = block.receivedTime,
        timestamp = block.block.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()
    )

    override fun withReceivedTime(value: Instant): FlowBlockchainBlock {
        return copy(receivedTime = value)
    }
}
