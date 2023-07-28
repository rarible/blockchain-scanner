package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.core.entity.reducer.model.Entity
import com.rarible.core.test.data.randomString
import org.springframework.data.annotation.Version

data class Item(
    override val id: String,
    override val revertableEvents: List<ItemEvent> = emptyList(),
    @Version
    override val version: Long? = null
) : Entity<String, ItemEvent, Item> {
    override fun withRevertableEvents(events: List<ItemEvent>): Item {
        return copy(revertableEvents = events)
    }
}

fun createRandomItem() = Item(id = randomString())
