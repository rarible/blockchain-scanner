package com.rarible.blockchain.scanner.v2.event

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

sealed class BlockEvent {
    abstract val number: Long
    abstract val hash: String
}

data class NewBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent() {
    constructor(block: BlockchainBlock) : this(block.number, block.hash)
}

data class RevertedBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent() {
    constructor(block: BlockchainBlock) : this(block.number, block.hash)
}