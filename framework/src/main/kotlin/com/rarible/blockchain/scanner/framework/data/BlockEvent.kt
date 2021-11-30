package com.rarible.blockchain.scanner.framework.data

sealed class BlockEvent {
    abstract val source: Source
    abstract val number: Long
    abstract val hash: String

    override fun toString(): String {
        return "$number:$hash"
    }
}

data class NewBlockEvent(
    override val source: Source,
    override val number: Long,
    override val hash: String
) : BlockEvent()

data class RevertedBlockEvent(
    override val source: Source,
    override val number: Long,
    override val hash: String
) : BlockEvent()

enum class Source {
    BLOCKCHAIN,
    PENDING,
    REINDEX
}
