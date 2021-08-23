package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.bouncycastle.util.encoders.Hex
import com.nftco.flow.sdk.FlowBlock
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.FieldType
import org.springframework.data.mongodb.core.mapping.MongoId
import java.time.ZoneOffset

@Document
data class FlowBlock(
    @field:MongoId(FieldType.INT64)
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
