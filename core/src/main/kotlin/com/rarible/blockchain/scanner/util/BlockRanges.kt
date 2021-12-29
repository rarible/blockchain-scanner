package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.data.BlockEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

object BlockRanges {

    /**
     * Chunks consequent block events by type.
     */
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

    fun getRanges(from: Long, to: Long, step: Int): Flow<LongRange> =
        (from..to).chunked(step) {
            LongRange(it.first(), it.last())
        }.asFlow()

    fun toRanges(blockNumbers: List<Long>): List<LongRange> {
        if (blockNumbers.isEmpty()) {
            return emptyList()
        }
        val result = mutableListOf<LongRange>()

        val iter = blockNumbers.iterator()
        var start = iter.next()
        var current = start

        while (iter.hasNext()) {
            val next = iter.next()
            if (next != current + 1) {
                result.add(LongRange(start, current))
                start = next
            }
            current = next
        }

        result.add(LongRange(start, current))
        return result
    }
}
