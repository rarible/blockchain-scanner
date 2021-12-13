package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.data.BlockEvent

object BlockBatcher {

    fun toBatches(events: List<BlockEvent>): List<List<BlockEvent>> {
        val batches = mutableListOf<List<BlockEvent>>()
        val iterator = events.iterator()
        var current = iterator.next()
        var currentBatch = mutableListOf(current)
        while (iterator.hasNext()) {
            val next = iterator.next()
            if (next.javaClass == current.javaClass) {
                currentBatch.add(next)
            } else {
                batches.add(currentBatch)
                currentBatch = mutableListOf(next)
            }
            current = next
        }
        batches.add(currentBatch)
        return batches
    }

}