package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.data.annotation.Id

data class TestBlock(
    @Id
    override val id: Long,

    override val hash: String,
    override val parentHash: String?,
    override val timestamp: Long,
    override val status: Block.Status,
    val extra: String
) : Block