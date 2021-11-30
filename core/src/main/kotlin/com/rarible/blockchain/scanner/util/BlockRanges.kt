package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

object BlockRanges {
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
