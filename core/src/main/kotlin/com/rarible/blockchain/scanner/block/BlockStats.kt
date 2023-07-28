package com.rarible.blockchain.scanner.block

import com.rarible.core.common.nowMillis
import java.time.Instant
import java.util.TreeMap

data class BlockStats(
    val updatedAt: Instant,
    val inserted: Int,
    val updated: Int,
    val subscribers: Map<String, SubscriberStats>
) {

    fun merge(other: BlockStats?): BlockStats {
        if (other == null) {
            return this
        }
        val mergedSubscribers = TreeMap<String, SubscriberStats>()
        val left = HashMap(other.subscribers)
        this.subscribers.forEach {
            mergedSubscribers[it.key] = it.value.merge(left.remove(it.key))
        }
        mergedSubscribers.putAll(left)

        return BlockStats(
            updatedAt = nowMillis(),
            updated = this.updated + other.updated,
            inserted = this.inserted + other.inserted,
            subscribers = mergedSubscribers
        )
    }
}

data class SubscriberStats(
    val inserted: Int,
    val updated: Int
) {

    fun merge(other: SubscriberStats?): SubscriberStats {
        if (other == null) {
            return this
        }
        return SubscriberStats(
            updated = this.updated + other.updated,
            inserted = this.inserted + other.inserted,
        )
    }
}
