package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

object BlockRanges {

    /**
     * Chunks consequent block events by type.
     */
    fun <BB : BlockchainBlock> toBatches(events: List<BlockEvent<BB>>): List<List<BlockEvent<BB>>> {
        val batches = mutableListOf<List<BlockEvent<BB>>>()
        val iterator = events.iterator()
        var current = iterator.next()
        var currentBatch = mutableListOf(current)

        while (iterator.hasNext()) {
            val next = iterator.next()
            if (next.javaClass == current.javaClass) {
                currentBatch += next
            } else {
                batches += currentBatch
                currentBatch = mutableListOf(next)
            }
            current = next
        }
        batches += currentBatch
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
                result += LongRange(start, current)
                start = next
            }
            current = next
        }

        result += LongRange(start, current)
        return result
    }
}
