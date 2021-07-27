package com.rarible.blockchain.scanner.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

data class BlockMeta(
    val number: Long,
    val hash: String,
    val parentHash: String?,
    val timestamp: Long
) {

    constructor(block: BlockchainBlock) :
            this(
                number = block.number,
                hash = block.hash,
                parentHash = block.parentHash,
                timestamp = block.timestamp
            )
}


