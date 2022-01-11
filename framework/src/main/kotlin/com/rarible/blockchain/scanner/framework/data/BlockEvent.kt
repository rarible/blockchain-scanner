package com.rarible.blockchain.scanner.framework.data

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock

sealed class BlockEvent<BB : BlockchainBlock> {
    abstract val number: Long
    abstract val hash: String
}

sealed class NewBlockEvent<BB : BlockchainBlock> : BlockEvent<BB>() {
    abstract val block: BB
    override val number: Long
        get() = block.number
    override val hash: String
        get() = block.hash

    final override fun toString(): String = "stable:$number:$hash"
}

data class NewStableBlockEvent<BB : BlockchainBlock>(
    override val block: BB
) : NewBlockEvent<BB>()

data class NewUnstableBlockEvent<BB : BlockchainBlock>(
    override val block: BB
) : NewBlockEvent<BB>()

data class RevertedBlockEvent<BB : BlockchainBlock>(
    override val number: Long,
    override val hash: String
) : BlockEvent<BB>() {
    override fun toString(): String = "revert:$number:$hash"
}
