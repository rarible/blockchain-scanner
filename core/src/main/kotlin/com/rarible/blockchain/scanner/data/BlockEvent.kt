package com.rarible.blockchain.scanner.data

import com.fasterxml.jackson.annotation.JsonIgnore
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class BlockEvent(
    val block: BlockMeta,
    val reverted: BlockMeta? = null
) {

    constructor(blockchainBlock: BlockchainBlock) : this(BlockMeta(blockchainBlock))

    @get:JsonIgnore
    val contextParams: Map<String, String>
        get() = mapOf(
            "blockNumber" to block.number.toString(),
            "blockHash" to block.hash,
            "eventType" to "newBlock"
        ) + if (reverted != null) mapOf("reverted" to reverted.hash) else emptyMap()
}