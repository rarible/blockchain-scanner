package com.rarible.blockchain.scanner.framework.data

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes(
    JsonSubTypes.Type(name = "NEW", value = NewBlockEvent::class),
    JsonSubTypes.Type(name = "REVERTED", value = RevertedBlockEvent::class),
    JsonSubTypes.Type(name = "REINDEX", value = RevertedBlockEvent::class)
)
sealed class BlockEvent {
    abstract val source: Source
    abstract val number: Long
}

data class NewBlockEvent(
    override val source: Source,
    override val number: Long,
    val hash: String
) : BlockEvent() {
    override fun toString(): String {
        return "$number:$hash:$source"
    }
}

data class RevertedBlockEvent(
    override val source: Source,
    override val number: Long,
    val hash: String
) : BlockEvent() {
    override fun toString(): String {
        return "$number:$hash:$source"
    }
}

data class ReindexBlockEvent(
    override val source: Source = Source.REINDEX,
    override val number: Long
) : BlockEvent() {
    override fun toString(): String {
        return "$number:$source"
    }
}

enum class Source {
    BLOCKCHAIN,
    PENDING,
    REINDEX
}
