package com.rarible.blockchain.scanner.framework.data

sealed class BlockEvent {
    abstract val number: Long
    abstract val hash: String
}

data class StableBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent() {
    override fun toString(): String = "stable:$number:$hash"
}

data class NewBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent() {
    override fun toString(): String = "new:$number:$hash"
}

data class RevertedBlockEvent(
    override val number: Long,
    override val hash: String
) : BlockEvent() {
    override fun toString(): String = "revert:$number:$hash"
}
