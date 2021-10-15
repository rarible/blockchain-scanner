package com.rarible.blockchain.scanner.framework.data

import com.fasterxml.jackson.annotation.JsonIgnore
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.BlockMeta

data class BlockEvent(
    val eventSource: Source,
    val block: BlockMeta,
    val reverted: BlockMeta? = null
) {

    constructor(eventSource: Source, blockchainBlock: BlockchainBlock) : this(eventSource, BlockMeta(blockchainBlock))

    @get:JsonIgnore
    val contextParams: Map<String, String>
        get() = mapOf(
            "blockNumber" to block.number.toString(),
            "blockHash" to block.hash,
            "eventType" to "newBlock"
        ) + if (reverted != null) mapOf("reverted" to reverted.hash) else emptyMap()
}

enum class Source {
    BLOCKCHAIN,
    PENDING,
    REINDEX
}
