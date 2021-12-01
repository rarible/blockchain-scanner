package com.rarible.blockchain.scanner.framework.data

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes(
    JsonSubTypes.Type(name = "NEW", value = NewBlockEvent::class),
    JsonSubTypes.Type(name = "REVERTED", value = RevertedBlockEvent::class)
)
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
