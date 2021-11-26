package com.rarible.blockchain.scanner.v2

sealed class BlockEvent {
    abstract val number: Long
    abstract val hash: String
}

data class NewBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent()

data class RevertedBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent()