package com.rarible.blockchain.scanner.framework.data

sealed class BlockEvent {
    abstract val source: Source
    abstract val number: Long
    abstract val hash: String
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
/*
    @get:JsonIgnore
    val contextParams: Map<String, String>
        get() = mapOf(
            "blockNumber" to block.number.toString(),
            "blockHash" to block.hash,
            "eventType" to "newBlock"
        ) + if (reverted != null) mapOf("reverted" to reverted.hash) else emptyMap()
}*/

enum class Source {
    BLOCKCHAIN,
    PENDING,
    REINDEX
}
