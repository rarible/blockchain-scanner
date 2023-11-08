package com.rarible.blockchain.scanner.flow.model

import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.asLocalDateTime
import org.onflow.protobuf.entities.BlockHeaderOuterClass
import java.time.LocalDateTime

data class FlowBlockHeader(
    val id: FlowId,
    val parentId: FlowId,
    val height: Long,
    val timestamp: LocalDateTime,
) {
    companion object {
        fun of(value: FlowBlock): FlowBlockHeader {
            return FlowBlockHeader(
                id = value.id,
                parentId = value.parentId,
                height = value.height,
                timestamp = value.timestamp
            )
        }

        fun of(value: BlockHeaderOuterClass.BlockHeader): FlowBlockHeader = FlowBlockHeader(
            id = FlowId.of(value.id.toByteArray()),
            parentId = FlowId.of(value.parentId.toByteArray()),
            height = value.height,
            timestamp = value.timestamp.asLocalDateTime()
        )
    }
}
