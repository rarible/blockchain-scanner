package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.FieldType
import org.springframework.data.mongodb.core.mapping.MongoId
import java.time.Instant

@Document
data class FlowBlock(
    @field:MongoId(FieldType.INT64)
    override val id: Long,
    @Indexed(unique = true)
    override val hash: String,
    @Indexed
    override val parentHash: String,
    override val timestamp: Long,
): Block {

    override fun timestamp(): Instant = Instant.ofEpochMilli(timestamp)
}
