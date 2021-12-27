package com.rarible.blockchain.scanner.framework.data

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes(
    JsonSubTypes.Type(name = "NEW", value = NewBlockEvent::class),
    JsonSubTypes.Type(name = "REVERTED", value = RevertedBlockEvent::class),
    JsonSubTypes.Type(name = "REINDEX", value = ReindexBlockEvent::class)
)
sealed class BlockEvent {
    abstract val number: Long
}

data class NewBlockEvent(
    override val number: Long,
    val hash: String
) : BlockEvent() {
    override fun toString(): String = "$number:$hash"
}

data class RevertedBlockEvent(
    override val number: Long,
    val hash: String
) : BlockEvent() {
    override fun toString(): String = "$number:$hash"
}

data class ReindexBlockEvent(
    override val number: Long
) : BlockEvent() {
    override fun toString(): String = "$number"
}
